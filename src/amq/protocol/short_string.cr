module AMQ
  module Protocol
    struct ShortString
      POOL = StringPool.new

      def initialize(@str : String)
      end

      def to_io(io, format = nil)
        raise ArgumentError.new("Short string too long, max #{UInt8::MAX}") if @str.bytesize > UInt8::MAX
        io.write_byte(@str.bytesize.to_u8)
        io.write(@str.to_slice)
      end

      def self.from_io(io, format) : String
        sz = io.read_byte || raise IO::EOFError.new("Can't read short string")
        buf = uninitialized UInt8[256]
        io.read_fully(buf.to_slice[0, sz])
        POOL.get(buf.to_unsafe, sz)
      end
    end
  end
end
