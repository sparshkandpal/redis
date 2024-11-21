# frozen_string_literal: true

require 'socket'
require 'base64'

class YourRedisServer
  def initialize(port, port_details, master_port, master_host)
    @port = port
    @port_details = port_details
    @master_port = master_port
    @master_host = master_host
    @client_buffers = {}
    @replication_buffer = String.new  # Mutable replication buffer
    @in_replication_mode = true       # Start in replication mode for RDB transfer
    @port_details['master_replid'] = '8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb'
    @port_details['master_repl_offset'] = '0'
  end

  def start
    server = TCPServer.new(@port)
    @clients = []
    @store = {}
    @expiry = {}
    @multi = {}
    @slave_sockets = []

    do_handshake(@master_host, @master_port, @port) if @master_port

    loop do
      # Add server and clients to watch list
      fds_to_watch = [server, *@clients]
      fds_to_watch << @replication_socket if @replication_socket
      ready_to_read, _, _ = IO.select(fds_to_watch)

      ready_to_read.each do |ready|
        if ready == server
          new_client = server.accept
          @clients << new_client
          @client_buffers[new_client] = String.new  # Use mutable string for client buffers
        elsif ready == @replication_socket
          handle_replication
        else
          handle_client(ready)
        end
      end
    end
  end

  def handle_client(client)
    begin
      @client_buffers[client] ||= String.new

      # Read incoming data and append to the buffer
      @client_buffers[client] << client.readpartial(1024)

      # Process all complete commands in the buffer
      while (inputs = extract_command_from_buffer(@client_buffers[client]))
        response, response_type = handle_request(client, inputs)

        if response_type == 'read'
          client.write(response)
        end

        if @port_details[@port] == 'master' && response_type == 'Write'
          client.write(response)
          @slave_sockets.each do |socket|
            begin
              socket.write(format_as_resp_array(inputs))
            rescue Errno::EPIPE, Errno::ECONNRESET
              puts "Lost connection to slave. Removing it from list."
              @slave_sockets.delete(socket)
            end
          end
        end
      end

    rescue EOFError
      @clients.delete(client)
      @client_buffers.delete(client)
      client.close
    end
  end

  def handle_replication
    begin
      # Read from replication socket and append to buffer
      @replication_buffer << @replication_socket.readpartial(1024)

      if @in_replication_mode
        # Check if RDB file is completely received
        if @replication_buffer.include?("\r\n")
          # Assume RDB ends at the presence of "\r\n" for now
          @in_replication_mode = false
          @replication_buffer.clear  # Clear the buffer to prepare for subsequent commands
        end
      else
        # Process all complete commands in the replication buffer
        while (inputs = extract_command_from_buffer(@replication_buffer))
          handle_request(@replication_socket, inputs)
        end
      end
    rescue EOFError
      @replication_socket.close
      @replication_socket = nil
    end
  end

  def extract_command_from_buffer(buffer)
    return nil if buffer.empty?

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
    elsif inputs[0].casecmp("EXEC").zero?
      if @multi[client] && @multi[client].size() > 0
        response = "*#{@multi[client].size}\r\n"
        multi_inputs = @multi[client]
        @multi.delete(client)
        multi_inputs.each do |inputs|
          exec_response, exec_response_type = handle_request(client, inputs)
          response = response + exec_response
        end
      elsif !@multi[client]
        response = "-ERR EXEC without MULTI\r\n"
      else
        @multi[client] = nil
        response = "*0\r\n"
      end
      response_type = 'Write'
    elsif inputs[0].casecmp("MULTI").zero?
      @multi[client] = []
      response = "+OK\r\n"
      response_type = 'Write'
    elsif inputs[0].casecmp("INFO").zero? && inputs[1].casecmp("replication").zero?
      port_details = @port_details[@port]
      master_replid = @port_details['master_replid']
      master_repl_offset = @port_details['master_repl_offset']

      if port_details == 'slave'
        data = "role:slave"
        response = "$#{data.bytesize}\r\n#{data}\r\n"
      else
        data = "role:master\r\nmaster_replid:#{master_replid}\r\nmaster_repl_offset:#{master_repl_offset}"
        response = "$#{data.bytesize}\r\n#{data}\r\n"
      end
      response_type = 'read'
    elsif inputs[0].casecmp("PING").zero?
      response = "+PONG\r\n"
      response_type = 'read'
    elsif inputs[0].casecmp("REPLCONF").zero?
      if inputs[1] == 'listening-port'
        @slave_sockets.push(client) unless @slave_sockets.include?(client)
      end
      response = "+OK\r\n"
      response_type = 'read'
    elsif inputs[0].casecmp("PSYNC").zero?
      full_resync_response = "+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n"

      x = Base64.decode64('UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==')
      replication_data = "$#{x.length}\r\n#{x}"

      response = full_resync_response + replication_data
      response_type = 'read'
    elsif inputs[0].casecmp("ECHO").zero?
      message = inputs[1]
      response = "$#{message.bytesize}\r\n#{message}\r\n"
      response_type = 'read'
    elsif inputs[0].casecmp("SET").zero?
      @expiry[inputs[1]] = Time.now + (inputs.last.to_i / 1000.to_f) if inputs[3]
      @store[inputs[1]] = inputs[2]
      response = "+OK\r\n"
      response_type = 'Write'
    elsif inputs[0].casecmp("GET").zero?
      message = @store[inputs[1]]

      if message.nil? || (@expiry[inputs[1]] && @expiry[inputs[1]] < Time.now)
        response = "$-1\r\n"
      else
        response = "$#{message.to_s.bytesize}\r\n#{message}\r\n"
      end
      response_type = 'read'
    elsif inputs[0].casecmp("INCR").zero?
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
      response_type = 'read'
    end

    return response, response_type
  end
end

def parse_port
  port_details = {}
  master_port = nil
  master_host = nil

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

  [port, port_details, master_port, master_host]
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
  socket.readpartial(1024)
  @replication_socket = socket
end

# Start the server
port, port_details, master_port, master_host = parse_port
YourRedisServer.new(port, port_details, master_port, master_host).start
