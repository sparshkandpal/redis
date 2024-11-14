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

    if inputs[0].casecmp("PING").zero?
      client.write("+PONG\r\n")
    elsif inputs[0].casecmp("ECHO").zero?
      message = inputs[1] 
      response = "$#{message.bytesize}\r\n#{message}\r\n"
      client.write(response)
    elsif inputs[0].casecmp("SET").zero?
      @expiry[inputs[1]] = Time.now + (inputs.last.to_i/1000.to_f) if inputs[3]
      @store[inputs[1]] = inputs[2]

      client.write("+OK\r\n")
    elsif inputs[0].casecmp("GET").zero?
      message = @store[inputs[1]]

      if @expiry[inputs[1]] && @expiry[inputs[1]] < Time.now
        client.write("$-1\r\n")
      else
        client.write("+#{message}\r\n")
      end
    elsif inputs[0].casecmp("INCR").zero?
      if @store[inputs[1]] &&  @store[inputs[1]].to_s.match?(/\A-?\d+\z/)
        @store[inputs[1]] = @store[inputs[1]].to_i + 1
        client.write(":#{@store[inputs[1]]}\r\n")
      elsif @store[inputs[1]]
        client.write("-ERR value is not an integer or out of range\r\n")
      else
        @store[inputs[1]] = 1
        client.write(":#{@store[inputs[1]]}\r\n")
      end
    end

  rescue EOFError
    # If client disconnected, remove it from the clients list and close the socket
    @clients.delete(client)
    client.close
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

YourRedisServer.new(6379).start
