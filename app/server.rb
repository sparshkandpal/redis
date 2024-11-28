require 'socket'
require 'base64'
require 'fileutils'

class YourRedisServer
  def initialize(port, port_details, master_port, master_host, filepath, filename)
    @port = port
    @port_details = port_details
    @master_port = master_port
    @master_host = master_host
    @filepath = filepath
    @filename = filename
    @client_buffers = {}
    @replication_buffer = String.new  # Mutable replication buffer
    @in_replication_mode = true       # Start in replication mode for RDB transfer
    @port_details['master_replid'] = '8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb'
    @port_details['master_repl_offset'] = '0'
    @clients = []
    @store = {}
    @expiry = {}
    @multi = {}
    @slave_sockets = []
    @replica_offset = 0
    @all_replica_offset = {} # Track the number of bytes processed by replica
    @valid_stream_time = 0
    @valid_stream_sequence = 0
    @blocked_clients = {}
    @blocked_clients_mutex = Mutex.new

    load_rdb_file
  end

  def start
    do_handshake(@master_host, @master_port, @port) if @master_port
    server = TCPServer.new(@port)

    loop do
      # Calculate the nearest timeout for blocked clients
      timeout = calculate_block_timeout

      # Add server and clients to watch list
      fds_to_watch = [server, *@clients]
      fds_to_watch << @replication_socket if @replication_socket
      ready_to_read, _, _ = IO.select(fds_to_watch, nil, nil, timeout)

      if ready_to_read
        # Process ready sockets
        if @replication_socket && ready_to_read.include?(@replication_socket)
          handle_replication
          ready_to_read.delete(@replication_socket)
        end

        ready_to_read.each do |ready|
          if ready == server
            new_client = server.accept
            @clients << new_client
            @client_buffers[new_client] = String.new
          else
            handle_client(ready)
          end
        end
      else
        # IO.select timed out, handle timeouts
        check_blocked_clients_for_timeouts
      end
    end
  end

  def calculate_block_timeout
    return nil if @blocked_clients.empty?
    now = Time.now
    timeouts = @blocked_clients.values.flatten.map { |client_info| client_info[:block_time] - now }
    timeout = timeouts.min
    timeout > 0 ? timeout : 0
  end

  def handle_client(client)
    begin
      @client_buffers[client] ||= String.new

      # Read incoming data and append to the buffer
      @client_buffers[client] << client.readpartial(1024)

      # Process all complete commands in the buffer
      while (inputs = extract_command_from_buffer(@client_buffers[client]))
        response, response_type = handle_request(client, inputs)

        if response
          client.write(response)
        end

        if @port_details[@port] == 'master' && response_type == 'Write'
          @slave_sockets.each do |socket|
            begin
              socket.write(format_as_resp_array(inputs))
            rescue Errno::EPIPE, Errno::ECONNRESET, IOError
              @slave_sockets.delete(socket)
            end
          end
        end
      end
    rescue EOFError, IOError => e
      puts "Client disconnected: #{client.inspect} (#{e.message})"
      @clients.delete(client)
      @client_buffers.delete(client)
      client.close
      # Remove client from any blocked lists
      remove_client_from_blocked_list(client)
    end
  end

  def handle_replication
    begin
      # Read from replication socket and append to buffer
      data = @replication_socket.readpartial(1024)
      @replication_buffer << data

      if @in_replication_mode
        # Process any status lines starting with '+'
        while @replication_buffer.start_with?('+')
          end_of_line = @replication_buffer.index("\r\n")
          if end_of_line
            status_line = @replication_buffer.slice!(0..end_of_line+1)
            puts "Received status line: #{status_line.strip}"
            # Optionally parse the master replication ID and offset
            if status_line.start_with?("+FULLRESYNC")
              parts = status_line.strip.split
              if parts.length >= 3
                @port_details['master_replid'] = parts[1]
                @port_details['master_repl_offset'] = parts[2]
                puts "Master replid: #{@port_details['master_replid']}, offset: #{@port_details['master_repl_offset']}"
              end
            end
          else
            # Wait for more data
            return
          end
        end

        # Now, expect the RDB data starting with '$'
        if @replication_buffer[0] == '$'
          # Existing code to process RDB data
          end_of_length = @replication_buffer.index("\r\n")
          if end_of_length
            length_str = @replication_buffer[1...end_of_length]
            length = length_str.to_i
            rdb_data_start = end_of_length + 2
            total_length = rdb_data_start + length  # No extra \r\n after contents

            if @replication_buffer.bytesize >= total_length
              # Extract the RDB data
              rdb_data = @replication_buffer[rdb_data_start, length]
              # Process the RDB data if needed (e.g., load into your in-memory store)

              # Remove the RDB data from the buffer
              @replication_buffer.slice!(0...total_length)
              @in_replication_mode = false
              @replica_offset = 0
              puts "RDB processed, switching to command replication mode."
            else
              # Wait for more data
              return
            end
          else
            # Wait for more data (length line not complete)
            return
          end
        else
          @replication_socket.close
          @replication_socket = nil
          return
        end
      else
        # Increment the replica offset for the raw bytes received
        @replica_offset += data.bytesize
      end

      # Process all complete commands in the replication buffer
      while (inputs = extract_command_from_buffer(@replication_buffer))
        response, response_type = handle_request(nil, inputs)

        if response_type == 'Write'
          puts "Applied replicated command: #{inputs.inspect} with response: #{response}"
        elsif response_type == 'ack'
          @replication_socket.write(response)
        end
      end
    rescue EOFError
      @replication_socket.close
      @replication_socket = nil
    end
  end

  def extract_command_from_buffer(buffer)
    return nil if buffer.empty?

    if buffer[0] != '*'
      # Invalid protocol
      return nil
    end

    idx = 1
    end_of_number = buffer.index("\r\n", idx)
    return nil if end_of_number.nil?

    number_of_arguments = buffer[idx...end_of_number].to_i
    idx = end_of_number + 2

    parsed_arguments = []

    number_of_arguments.times do
      return nil if buffer[idx] != "$"

      end_of_length = buffer.index("\r\n", idx + 1)
      return nil if end_of_length.nil?

      length_of_argument = buffer[(idx + 1)...end_of_length].to_i
      idx = end_of_length + 2

      return nil if buffer.size < (idx + length_of_argument + 2)

      argument = buffer[idx...(idx + length_of_argument)]
      parsed_arguments << argument

      idx += length_of_argument + 2
    end

    # Remove the processed command from the buffer
    buffer.slice!(0...idx)
    puts "Extracted command: #{parsed_arguments.inspect} from buffer"
    parsed_arguments
  end

  def format_as_resp_array(inputs)
    resp_command = "*#{inputs.size}\r\n"
    inputs.each do |input|
      resp_command += "$#{input.bytesize}\r\n#{input}\r\n"
    end
    resp_command
  end

  def handle_request(client, inputs)
    response = ''
    response_type = 'read'

    if inputs[0].casecmp("DISCARD").zero?
      if @multi[client]
        @multi[client] = nil
        response = "+OK\r\n"
      else
        response = "-ERR DISCARD without MULTI\r\n"
      end
      response_type = 'Write'
    elsif @multi[client] && !inputs[0].casecmp("EXEC").zero?
      @multi[client] << inputs
      response = "+QUEUED\r\n"
      response_type = 'Write'
    elsif inputs[0].casecmp('EXEC').zero?
      if @multi[client] && @multi[client].size > 0
        response = "*#{@multi[client].size}\r\n"
        multi_inputs = @multi[client]
        @multi.delete(client)
        multi_inputs.each do |inputs|
          exec_response, _ = handle_request(client, inputs)
          response += exec_response
        end
      elsif !@multi[client]
        response = "-ERR EXEC without MULTI\r\n"
      else
        @multi[client] = nil
        response = "*0\r\n"
      end
      response_type = 'Write'
    elsif inputs[0].casecmp('MULTI').zero?
      @multi[client] = []
      response = "+OK\r\n"
      response_type = 'Write'
    elsif inputs[0].casecmp('INFO').zero? && inputs[1]&.casecmp('replication').zero?
      port_details = @port_details[@port]
      master_replid = @port_details['master_replid']
      master_repl_offset = @replica_offset.to_s

      if port_details == 'slave'
        data = 'role:slave'
        response = "$#{data.bytesize}\r\n#{data}\r\n"
      else
        data = "role:master\r\nmaster_replid:#{master_replid}\r\nmaster_repl_offset:#{master_repl_offset}"
        response = "$#{data.bytesize}\r\n#{data}\r\n"
      end
    elsif inputs[0].casecmp('TYPE').zero?
      if @store[inputs[1]].class == Array
        response = "+stream\r\n"
      elsif @store[inputs[1]]
        response = "+string\r\n"
      else
        response = "+none\r\n"
      end
    elsif inputs[0].casecmp('XREAD').zero?
      # Parse XREAD command
      block_index = inputs.index { |arg| arg.downcase == 'block' }
      block_timeout_ms = block_index ? inputs[block_index + 1].to_i : 0

      streams_index = inputs.index { |arg| arg.downcase == 'streams' }
      stream_keys = inputs[(streams_index + 1)...(streams_index + 1 + (inputs.size - streams_index - 1) / 2)]
      stream_ids = inputs[-stream_keys.size..-1]

      output_array = []
      messages_found = false

      stream_keys.each_with_index do |stream_key, index|
        messages = get_messages(stream_key, stream_ids[index])
        if messages.any?
          messages_found = true
          output_array << [stream_key, messages]
        end
      end

      if messages_found
        # Messages are available, send them immediately
        response = to_redis_resp(output_array)
      elsif block_timeout_ms > 0
        # No messages, block the client
        block_time = Time.now + block_timeout_ms / 1000.0

        @blocked_clients_mutex.synchronize do
          stream_keys.each_with_index do |stream_key, index|
            @blocked_clients[stream_key] ||= []
            @blocked_clients[stream_key] << {
              client: client,
              stream_id: stream_ids[index],
              block_time: block_time
            }
          end
        end

        # Do not send a response now; client is blocked
        response = nil
      else
        # No messages and no blocking, return null bulk string
        response = "$-1\r\n"
      end
    elsif inputs[0].casecmp('XADD').zero?
      key = inputs[1]
      stream_id = inputs[2]

      # Handle auto-generated IDs if necessary
      if stream_id == '*'
        timestamp = (Time.now.to_f * 1000).to_i
        sequence = 0
        stream_id = "#{timestamp}-#{sequence}"
      end

      stream_id_array = inputs[2].split('-')
      if stream_id_array[1] == '*'
        if stream_id_array[0].to_i == 0
          stream_id_array[1] = 1
        elsif stream_id_array[0].to_i > @valid_stream_time
          stream_id_array[1] = 0
        else
          stream_id_array[1] = @valid_stream_sequence + 1
        end
        stream_id = "#{stream_id_array[0]}-#{stream_id_array[1]}"
      end

      # Validate stream ID
      if stream_id == '0-0'
        response = "-ERR The ID specified in XADD must be greater than 0-0\r\n"
      elsif @store[key] && compare_stream_ids(stream_id, @store[key].last[0]) <= 0
        response = "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"
      else
        @store[key] ||= []
        @store[key] << [stream_id, inputs[3..]]

        @valid_stream_time, @valid_stream_sequence = stream_id.split('-').map(&:to_i)

        response = "$#{stream_id.bytesize}\r\n#{stream_id}\r\n"

        # After adding the message, notify blocked clients
        notify_blocked_clients(key, stream_id)
      end
    elsif inputs[0].casecmp('XRANGE').zero?
      stream = @store[inputs[1]]
      start_time = inputs[2]
      end_time = inputs[3]

      if start_time == '-'
        start_time = @store[inputs[1]][0][0]
      end

      if end_time == '+'
        end_time = @store[inputs[1]].last[0]
      end

      output_array = []

      stream.each do |entry|
        timestamp = entry[0]
        data = entry[1]

        # Start saving when the timestamp matches or exceeds the start_time
        if timestamp >= start_time && timestamp <= end_time
          output_array << entry
        end

        # Stop when the end_time is exceeded
        break if timestamp > end_time
      end

      response = parse_array_response(output_array)
    elsif inputs[0].casecmp('WAIT').zero?
      response = ":#{@slave_sockets.size}\r\n"
    elsif inputs[0].casecmp('PING').zero?
      response = "+PONG\r\n"
    elsif inputs[0].casecmp('CONFIG').zero?
      if  inputs[2].casecmp("dir").zero?
        response = "*2\r\n$3\r\ndir\r\n$#{@filepath.bytesize}\r\n#{@filepath}\r\n"
      else
        response = "*2\r\n$10\r\ndbfilename\r\n$#{@filename.bytesize}\r\n#{@filename}\r\n"
      end
      response_type = "read"
    elsif inputs[0].casecmp('KEYS').zero?
      matching_keys = @store.keys

      response = "*#{matching_keys.size}\r\n"
      matching_keys.each do |key|
        response += "$#{key.bytesize}\r\n#{key}\r\n"
      end
    elsif inputs[0].casecmp('REPLCONF').zero?
      if inputs[1] == 'listening-port'
        if client
          @slave_sockets.push(client) unless @slave_sockets.include?(client)
          puts "Registered slave socket: #{client.inspect}"
        else
          puts "Warning: REPLCONF listening-port received with client=nil"
        end
        response = "+OK\r\n"
      elsif inputs[1] == 'GETACK'
        response = "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$#{@replica_offset.to_s.bytesize}\r\n#{@replica_offset}\r\n"
        response_type = 'ack'
      elsif inputs[1] == 'ACK'
        @all_replica_offset[client] == inputs[2]
        response_type = 'read'
      else
        response = "+OK\r\n"
      end
    elsif inputs[0].casecmp('PSYNC').zero?
      full_resync_response = "+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n"

      x = Base64.decode64('UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==')
      replication_data = "$#{x.length}\r\n#{x}"

      response = full_resync_response + replication_data
    elsif inputs[0].casecmp('ECHO').zero?
      message = inputs[1]
      response = "$#{message.bytesize}\r\n#{message}\r\n"
    elsif inputs[0].casecmp('SET').zero?
      @expiry[inputs[1]] = Time.now + (inputs.last.to_i / 1000.to_f) if inputs[3]
      @store[inputs[1]] = inputs[2]
      response = "+OK\r\n"
      response_type = 'Write'
    elsif inputs[0].casecmp('GET').zero?
      message = @store[inputs[1]]

      if message.nil? || (@expiry[inputs[1]] && @expiry[inputs[1]] < Time.now)
        response = "$-1\r\n"
      else
        response = "$#{message.to_s.bytesize}\r\n#{message}\r\n"
      end
    elsif inputs[0].casecmp('INCR').zero?
      if @store[inputs[1]] && @store[inputs[1]].to_s.match?(/\A-?\d+\z/)
        @store[inputs[1]] = @store[inputs[1]].to_i + 1
        response = ":#{@store[inputs[1]]}\r\n"
      elsif @store[inputs[1]]
        response = "-ERR value is not an integer or out of range\r\n"
      else
        @store[inputs[1]] = 1
        response = ":#{@store[inputs[1]]}\r\n"
      end
      response_type = 'Write'
    else
      response = "-ERR unknown command '#{inputs[0]}'\r\n"
    end

    [response, response_type]
  end
end

def parse_array_response(data)
  puts "data #{data}"
  puts "datasize #{data.size}"

  result = ""

  # Total number of top-level elements
  result << "*#{data.size}\r\n"

  puts "rs1 #{result.size}"
  data.each do |entry|
    timestamp, readings = entry

    result << "*2\r\n"
    result << "$#{timestamp.length}\r\n#{timestamp}\r\n"

    # Add the readings
    result << "*#{readings.size / 2}\r\n"
    readings.each_slice(2) do |key, value|
      result << "$#{key.length}\r\n#{key}\r\n"
      result << "$#{value.length}\r\n#{value}\r\n"
    end
  end

  puts "rs2 #{result.size}"
  result
end

def create_output_array(stream_key, stream_id)
  outer_array = [stream_key]
  inner_array = []

  stream = @store[stream_key]

  return if stream.nil?

  stream.each do |entry|
    timestamp = entry[0]
    puts "timestamp #{timestamp} / stream_id #{stream_id} / entry #{entry}"

    if timestamp <= stream_id
      next
    else
      inner_array << entry
    end
  end

  outer_array.push(inner_array)
  outer_array
end

def parse_port
  port_details = {}
  master_port = nil
  master_host = nil
  filepath = nil
  filename = nil

  dir_index = ARGV.index('--dir')

  filepath = ARGV[dir_index + 1] if dir_index

  dbfile_index = ARGV.index('--dbfilename')

  filename = ARGV[dbfile_index + 1] if dbfile_index

  port_flag_index = ARGV.index('--port')
  port = if port_flag_index && ARGV[port_flag_index + 1]
           ARGV[port_flag_index + 1].to_i
         else
           6379
         end

  replica_of_index = ARGV.index('--replicaof')
  if replica_of_index && ARGV[replica_of_index + 1]
    master_host = ARGV[replica_of_index + 1].split.first
    master_port = ARGV[replica_of_index + 1].split.last.to_i
    port_details[port] = 'slave'
  else
    port_details[port] = 'master'
  end

  [port, port_details, master_port, master_host, filepath, filename]
end

def do_handshake(host, port, listening_port)
  socket = TCPSocket.open(host, port)
  socket.write("*1\r\n$4\r\nPING\r\n")
  socket.readpartial(1024)
  socket.write("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n#{listening_port}\r\n")
  socket.readpartial(1024)
  socket.write("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")
  socket.readpartial(1024)
  socket.write("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
  # Do not read the response here; let handle_replication process it
  @replication_socket = socket
end

def to_redis_resp(obj)
  if obj.is_a?(Array)
    resp = "*#{obj.size}\r\n"
    obj.each do |element|
      resp += to_redis_resp(element)
    end
    resp
  else
    resp = "$#{obj.to_s.bytesize}\r\n#{obj}\r\n"
    resp
  end
end

  def load_rdb_file
    return unless @filepath && @filename

    rdb_file = File.join(@filepath, @filename)
    unless File.exist?(rdb_file)
      puts "RDB file #{rdb_file} does not exist. Starting with an empty database."
      return
    end

    File.open(rdb_file, 'rb') do |file|
      parse_rdb_file(file)
    end
    puts "RDB file #{rdb_file} loaded successfully."
  end

  def parse_rdb_file(file)
    # Read and validate the header
    magic_string = file.read(5)
    version = file.read(4)

    unless magic_string == 'REDIS' && version.match?(/\d{4}/)
      raise "Invalid RDB file format."
    end

    # Start parsing the body
    loop do
      byte = file.read(1)

      break unless byte

      puts "Parsing byte: #{byte.unpack('H*')[0]} at position #{file.pos - 1}"

      case byte.ord
      when 0xFA
        puts "Found metadata section"
        parse_metadata(file)
      when 0xFE
        puts "Found database section"
        db_number = parse_length(file)
        puts "Database number: #{db_number}"
      when 0xFB
        puts "Found hash table size info"
        main_ht_size = parse_length(file)
        puts "Main hash table size: #{main_ht_size}"
        expires_ht_size = parse_length(file)
        puts "Expires hash table size: #{expires_ht_size}"
      when 0xFD, 0xFC
        puts "Found key with expire"
        expiry = parse_expiry(file, byte.ord)
        value_type_byte = file.read(1)
        raise "Unexpected EOF while reading value type" unless value_type_byte
        value_type = value_type_byte.ord
        puts "Value type: #{value_type}"
        key = parse_string(file)
        puts "Key: #{key}"
        value = parse_value(file, value_type)
        @store[key] = value
        @expiry[key] = expiry if expiry
        puts "Loaded key with expire: #{key} => #{value}, expires at #{expiry}"
      when 0xFF
        puts "End of RDB file"
        checksum = file.read(8)
        raise "Unexpected EOF while reading checksum" unless checksum && checksum.length == 8
        break
      else
        value_type = byte.ord
        puts "Found key without expire, value type: #{value_type}"
        key = parse_string(file)
        puts "Key: #{key}"
        value = parse_value(file, value_type)
        @store[key] = value
        puts "Loaded key: #{key} => #{value}"
      end
    end
end

def parse_length(file)
  first_byte = file.read(1)
  raise "Unexpected EOF while parsing length" unless first_byte
  first_byte = first_byte.ord
  enc_type = (first_byte & 0xC0) >> 6
  case enc_type
  when 0
    # 6-bit length
    length = first_byte & 0x3F
    return [false, length]
  when 1
    # 14-bit length
    second_byte = file.read(1)
    raise "Unexpected EOF while parsing length (14-bit)" unless second_byte
    second_byte = second_byte.ord
    length = ((first_byte & 0x3F) << 8) | second_byte
    return [false, length]
  when 2
    # 32-bit length
    bytes = file.read(4)
    raise "Unexpected EOF while parsing length (32-bit)" unless bytes && bytes.length == 4
    length = bytes.unpack('N')[0]  # Big-endian unsigned 32-bit integer
    return [false, length]
  when 3
    # Special encoding
    encoding = first_byte & 0x3F
    return [true, encoding]
  else
    raise "Unknown length encoding in RDB file."
  end
end

def parse_string(file)
  is_encoded, length_or_enc = parse_length(file)
  if !is_encoded
    length = length_or_enc
    # Read the string of given length
    str = file.read(length)
    raise "Unexpected EOF while reading string" unless str && str.length == length
  else
    # Special encoding
    encoding_type = length_or_enc
    case encoding_type
    when 0  # 8-bit integer
      bytes = file.read(1)
      raise "Unexpected EOF while reading 8-bit integer" unless bytes
      int_value = bytes.unpack('C')[0]
      str = int_value.to_s
    when 1  # 16-bit integer
      bytes = file.read(2)
      raise "Unexpected EOF while reading 16-bit integer" unless bytes && bytes.length == 2
      int_value = bytes.unpack('s<')[0]
      str = int_value.to_s
    when 2  # 32-bit integer
      bytes = file.read(4)
      raise "Unexpected EOF while reading 32-bit integer" unless bytes && bytes.length == 4
      int_value = bytes.unpack('l<')[0]
      str = int_value.to_s
    when 3
      # LZF compressed string (not required for this challenge)
      raise "LZF compression not supported in this challenge."
    else
      raise "Unknown string encoding type #{encoding_type}."
    end
  end
  str
end

def parse_value(file, value_type)
  puts "Parsing value with type: #{value_type}"
  case value_type
  when 0 # String
    value = parse_string(file)
    puts "Parsed string value: #{value}"
    value
  else
    raise "Unsupported value type #{value_type}."
  end
end

def parse_expiry(file, byte)
  case byte
  when 0xFD # Expire time in seconds
    bytes = file.read(4)
    raise "Unexpected EOF while reading expiry in seconds" unless bytes && bytes.length == 4
    timestamp = bytes.unpack('L<')[0] # Little-endian unsigned 32-bit integer
    Time.at(timestamp)
  when 0xFC  # Expire time in milliseconds
    bytes = file.read(8)
    raise "Unexpected EOF while reading expiry in milliseconds" unless bytes && bytes.length == 8
    timestamp = bytes.unpack('Q<')[0] # Little-endian unsigned 64-bit integer
    Time.at(timestamp / 1000.0)
  else
    nil
  end
end

def get_messages(stream_key, stream_id)
  stream = @store[stream_key] || []

  messages = stream.select do |entry|
    id = entry[0]
    compare_stream_ids(id, stream_id) > 0
  end

  messages.map do |entry|
    id = entry[0]
    data = entry[1]
    [id, data]
  end
end

def compare_stream_ids(id1, id2)
  ts1, seq1 = id1.split('-').map(&:to_i)
  ts2, seq2 = id2.split('-').map(&:to_i)
  return ts1 <=> ts2 if ts1 != ts2
  seq1 <=> seq2
end

def parse_metadata(file)
  d_name = parse_string(file)
  value = parse_string(file)
  # You can store the metadata if needed, e.g.,
  # @metadata[name] = value
end

def check_blocked_clients_for_timeouts
  now = Time.now
  @blocked_clients.each do |stream_key, clients|
    clients.delete_if do |blocked_client|
      if blocked_client[:block_time] <= now
        # Timeout expired, send null bulk string
        client = blocked_client[:client]
        begin
          client.write("$-1\r\n")
        rescue IOError, Errno::EPIPE
          # Client might have disconnected
          @clients.delete(client)
          @client_buffers.delete(client)
          client.close
        end
        true # Remove this client from the list
      else
        false
      end
    end
  end
  # Remove any stream_keys with empty clients array
  @blocked_clients.delete_if { |_, clients| clients.empty? }
end

def remove_client_from_blocked_list(client)
  @blocked_clients.each do |stream_key, clients|
    clients.delete_if { |blocked_client| blocked_client[:client] == client }
  end
  @blocked_clients.delete_if { |_, clients| clients.empty? }
end

def notify_blocked_clients(stream_key, new_stream_id)
  @blocked_clients_mutex.synchronize do
    return unless @blocked_clients[stream_key]

    clients_to_remove = []

    @blocked_clients[stream_key].each do |blocked_client|
      if compare_stream_ids(new_stream_id, blocked_client[:stream_id]) > 0
        # New message is available for this client
        messages = get_messages(stream_key, blocked_client[:stream_id])
        if messages.any?
          response = to_redis_resp([[stream_key, messages]])
          begin
            blocked_client[:client].write(response)
          rescue IOError, Errno::EPIPE
            # Client might have disconnected
            @clients.delete(blocked_client[:client])
            @client_buffers.delete(blocked_client[:client])
            blocked_client[:client].close
          end
        end
        clients_to_remove << blocked_client
      end
    end

    # Remove unblocked clients from the blocked list
    @blocked_clients[stream_key] -= clients_to_remove
    @blocked_clients.delete(stream_key) if @blocked_clients[stream_key].empty?
  end
end

# Start the server
port, port_details, master_port, master_host, filepath, filename = parse_port
YourRedisServer.new(port, port_details, master_port, master_host, filepath, filename).start
