module AMQ
  module Protocol
    struct LongString
      def initialize(@str : String)
      end

      def to_io(io, format)
        raise ArgumentError.new("Long string is too long, max #{UInt32::MAX}") if @str.bytesize > UInt32::MAX
        io.write_bytes(@str.bytesize.to_u32, format)
        io.write(@str.to_slice)
      end

      def self.from_io(io, format) : String
        sz = UInt32.from_io(io, format)
        io.read_string(sz)
      end
    end
  end
end
