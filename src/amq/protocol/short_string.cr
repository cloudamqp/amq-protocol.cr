module AMQ
  module Protocol
    struct ShortString
      def initialize(@str : String)
      end

      def to_io(io, format = nil) : Nil
        raise ArgumentError.new("Short string too long, max #{UInt8::MAX}") if @str.bytesize > UInt8::MAX
        io.write_byte(@str.bytesize.to_u8)
        io.write(@str.to_slice)
      end

      def self.from_io(io, format = nil) : String
        buf = uninitialized UInt8[256]
        io.read(buf.to_slice[0, 1]) == 1 || raise IO::EOFError.new("Can't read short string")
        sz = buf[0]
        io.read_fully(buf.to_slice[0, sz])
        String.new(buf.to_unsafe, sz)
      end

      def self.from_bytes(bytes, format = nil) : String
        sz = bytes[0]
        String.new((bytes + 1).to_unsafe, sz)
      end

      def self.skip(io) : Nil
        sz = io.read_byte || raise IO::EOFError.new("Can't read short string")
        io.skip(sz)
      end
    end
  end
end
