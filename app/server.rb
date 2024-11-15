# frozen_string_literal: true

require 'socket'

class YourRedisServer
  def initialize(port)
    @port = port
  end

  def start
    server = TCPServer.new(@port) 
    @clients = []
    @store = {}
    @expiry = {}
    @multi = {}

    loop do
      # Add server and clients to watch list
      fds_to_watch = [server, *@clients]
      ready_to_read, _, _ = IO.select(fds_to_watch)

      ready_to_read.each do |ready|
        if ready == server
          # Accept new client and add to clients list
          @clients << server.accept
        else
          # Handle client request
          handle_client(ready)
        end
      end
    end
  end

  def handle_client(client)
    request = client.readpartial(1024)

    inputs = parser(request)

    if inputs[0].casecmp("DISCARD").zero?
      if @multi[client]
        @multi[client] = nil
        response = "+OK\r\n"
      else
        response = "-ERR DISCARD without MULTI\r\n"
      end
    elsif @multi[client] && !inputs[0].casecmp("EXEC").zero?
      @multi[client] << inputs
      response = "+QUEUED\r\n"
    elsif inputs[0].casecmp("EXEC").zero? && @multi[client] && @multi[client].size() > 0
      response = "*#{@multi[client].size}\r\n"
      @multi[client].each do |inputs|
        exec_response = handle_request(client, inputs)
        response = response + exec_response.strip + "\r\n"
      end
      @multi[client] = nil  # Clear the multi queue after execution
      response = response
    else
      response = handle_request(client, inputs)
    end
    client.write(response)
  rescue EOFError
    # If client disconnected, remove it from the clients list and close the socket
    @clients.delete(client)
    client.close
  end

  def handle_request(client, inputs)
    if inputs[0].casecmp("EXEC").zero?
      if !@multi[client]
       response = "-ERR EXEC without MULTI\r\n"
      elsif @multi[client].size.zero?
        @multi[client] = nil
        response = "*0\r\n"
      else ## to return an aray
        response = "-Will implement\r\n"
      end
    elsif inputs[0].casecmp("MULTI").zero?
      @multi[client] = []
      response = "+OK\r\n"
    elsif inputs[0].casecmp("PING").zero?
      response = "+PONG\r\n"
    elsif inputs[0].casecmp("ECHO").zero?
      message = inputs[1] 
      response = "$#{message.bytesize}\r\n#{message}\r\n"
    elsif inputs[0].casecmp("SET").zero?
      @expiry[inputs[1]] = Time.now + (inputs.last.to_i/1000.to_f) if inputs[3]
      @store[inputs[1]] = inputs[2]

      response = "+OK\r\n"
    elsif inputs[0].casecmp("GET").zero?
      message = @store[inputs[1]]

      if message.nil?
        response = "$-1\r\n"
      elsif @expiry[inputs[1]] && @expiry[inputs[1]] < Time.now
        response = "$-1\r\n"
      else
        response = "+#{message}\r\n"
      end
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
    end

    response
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
  port_flag_index = ARGV.index('--port')
  if port_flag_index && ARGV[port_flag_index + 1]
    ARGV[port_flag_index + 1].to_i
  else
    6379 # Default port
  end
end

port = parse_port
YourRedisServer.new(port).start
