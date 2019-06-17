module AMQ
  module Protocol
    struct ShortString
      getter bytesize
      @ptr : Pointer(UInt8)

      def initialize(@ptr : Pointer(UInt8), @bytesize : Int32)
      end

      def initialize(str : String)
        @ptr = str.to_unsafe
        @bytesize = str.bytesize
      end

      def to_unsafe
        @ptr
      end

      def inspect(io)
        io << "ShortString("
        io.write(@ptr.to_slice(@bytesize))
        io << ")"
      end

      def to_s
        String.new(@ptr, @bytesize)
      end

      def to_s(io)
        io.write(@ptr.to_slice(@bytesize))
      end

      def ==(other : self)
        return false unless bytesize == other.bytesize
        @ptr.memcmp(other.to_unsafe, bytesize) == 0
      end

      def to_io(io, format = nil)
        raise ArgumentError.new("Short string too long, max #{UInt8::MAX}") if @bytesize > UInt8::MAX
        io.write_byte(@bytesize.to_u8)
        io.write(@ptr.to_slice(@bytesize))
      end

      def self.from_io(io, format) : self
        bytesize = io.read_byte
        raise ::IO::EOFError.new("Can't read short string") if bytesize.nil?
        buf = uninitialized UInt8[256]
        io.read(buf.to_slice[0, bytesize])
        new(buf.to_unsafe, bytesize.to_i32)
      end
    end
  end
end
