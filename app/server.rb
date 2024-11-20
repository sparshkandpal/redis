# frozen_string_literal: true

require 'socket'
require 'base64'

class YourRedisServer
  def initialize(port, port_details)
    @port = port
    @port_details = port_details
  end

  def start
    server = TCPServer.new(@port) 
    @clients = []
    @store = {}
    @expiry = {}
    @multi = {}
    @slave_sockets = []

    loop do
      # Add server and clients to watch list
      fds_to_watch = [server, *@clients]
      ready_to_read, _, _ = IO.select(fds_to_watch)

      ready_to_read.each do |ready|
        if ready == server
          @clients << server.accept
        else
          handle_client(ready)
        end
      end
    end
  end

  def handle_client(client)
    request = client.readpartial(1024)

    inputs = parser(request)

    response, response_type = handle_request(client, inputs)

    if @port_details[@port] == 'master'
      client.write(response)
    end

    if @port_details[@port] == 'slave' && response_type == 'read'
      client.write(response)
    end

    if @port_details[@port] == 'master' && response_type == 'Write'
      @slave_sockets.each do |socket|
        begin
          socket.write(request) # Propagate command over the same socket
        rescue Errno::EPIPE, Errno::ECONNRESET
          puts "Lost connection to slave at port #{port}. Removing it from list."
        end
      end
    end

  rescue EOFError
    # If client disconnected, remove it from the clients list and close the socket
    @clients.delete(client)
    client.close
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
      else @multi[client].size.zero?
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
      port = inputs[2].to_i

      if inputs[1] == 'listening-port'
        begin
           @slave_sockets.push(client)
        rescue Errno::ECONNREFUSED
          response = "-ERR Failed to connect to slave\r\n"
        end
      end
      response = "+OK\r\n"
      response_type = 'read'
    elsif inputs[0].casecmp("PSYNC").zero?
      full_resync_response = "+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n"
  
      # Replication data, encoded as bulk response
      x = Base64.decode64('UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==')
      replication_data = "$#{x.length}\r\n#{x}" 
      
      # Concatenate the full resync response and replication data as a single response
      response = full_resync_response + replication_data
      response_type = 'read'
    elsif inputs[0].casecmp("ECHO").zero?
      message = inputs[1] 
      response = "$#{message.bytesize}\r\n#{message}\r\n"
      response_type = 'read'
    elsif inputs[0].casecmp("SET").zero?
      @expiry[inputs[1]] = Time.now + (inputs.last.to_i/1000.to_f) if inputs[3]
      @store[inputs[1]] = inputs[2]

      response = "+OK\r\n"
      response_type = 'Write'
    elsif inputs[0].casecmp("GET").zero?
      message = @store[inputs[1]]

      if message.nil?
        response = "$-1\r\n"
      elsif @expiry[inputs[1]] && @expiry[inputs[1]] < Time.now
        response = "$-1\r\n"
      else
        response = "+#{message}\r\n"
      end
      response_type = 'read'
    elsif inputs[0].casecmp("INCR").zero?
      if @store[inputs[1]] &&  @store[inputs[1]].to_s.match?(/\A-?\d+\z/)
        @store[inputs[1]] = @store[inputs[1]].to_i + 1
        response = ":#{@store[inputs[1]]}\r\n"
      elsif @store[inputs[1]]
        response = "-ERR value is not an integer or out of range\r\n"
      else
        @store[inputs[1]] = 1
        response = ":#{@store[inputs[1]]}\r\n"
      end
      response_type = 'Write'
    end

    return response, response_type
  end

  def parser(request)
    parsed_arguments = []

    idx = 1
    end_of_number  = request.index("\r\n", idx)
    number_of_arguments = request[idx...end_of_number].to_i

    idx = end_of_number + 2 

    number_of_arguments.times do

      idx+=1 if request[idx] == "$"

      end_of_length = request.index("\r\n", idx)
      length_of_argument = request[idx...end_of_length].to_i

      idx = end_of_length + 2

      argument = request[idx...(idx + length_of_argument)]
      parsed_arguments << argument

      idx += length_of_argument + 2
    end

    parsed_arguments
  end
end

  def parse_port
    port_details = {}

    port_flag_index = ARGV.index('--port')
    port = if port_flag_index && ARGV[port_flag_index + 1]
             ARGV[port_flag_index + 1].to_i
           else
             6379 # Default port
           end

    replica_of_index = ARGV.index('--replicaof')
    if replica_of_index && ARGV[replica_of_index + 1]
      master_host = ARGV[replica_of_index + 1].split.first
      master_port = ARGV[replica_of_index + 1].split.last.to_i
      port_details[port] = 'slave'
      do_handshake(master_host, master_port, port)
    else
      port_details[port] = 'master'
    end

    port_details['master_replid'] = '8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb'
    port_details['master_repl_offset'] = '0'

    [port, port_details]
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
  end

port, port_details = parse_port
YourRedisServer.new(port, port_details).start
