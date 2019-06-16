module AMQ
  module Protocol
    struct Table
      def initialize(@hash : Hash(ShortString, Field))
      end

      def self.from_io(io, format, size : UInt32? = nil) : Hash(ShortString, Field)
        size ||= UInt32.from_io(io, format)
        pos = 0
        hash = Hash(ShortString, Field).new
        while pos < size
          key = ShortString.from_io(io, format)
          val = read_field(io, format)
          pos += 1 + key.bytesize + field_bytesize(val)
          hash[key] = val
        end
        hash
      end

      def to_io(io, format)
        io.write_bytes(bytesize.to_u32 - 4, format)
        @hash.each do |key, value|
          io.write_bytes(key, format)
          write_field(value, io, format)
        end
      end

      @bytesize : UInt32? = nil

      def bytesize : UInt32
        return @bytesize.not_nil! if @bytesize
        size = 4_u32
        @hash.each do |key, value|
          size += 1_u32 + key.bytesize
          size += Table.field_bytesize(value)
        end
        @bytesize = size
        size
      end

      def self.field_bytesize(value : Field) : UInt32
        size = 1_u32
        case value
        when Bool
          size += sizeof(Bool)
        when Int8
          size += sizeof(Int8)
        when UInt8
          size += sizeof(UInt8)
        when Int16
          size += sizeof(Int16)
        when UInt16
          size += sizeof(UInt16)
        when Int32
          size += sizeof(Int32)
        when UInt32
          size += sizeof(UInt32)
        when Int64
          size += sizeof(Int64)
        when UInt64
          size += sizeof(UInt64)
        when Float32
          size += sizeof(Float32)
        when Float64
          size += sizeof(Float64)
        when ShortString
          size += 1 + value.bytesize
        when String
          size += sizeof(UInt32) + value.bytesize
        when Slice
          size += sizeof(UInt32) + value.bytesize
        when Array
          size += 4
          value.each do |v|
            size += field_bytesize(v)
          end
        when Time
          size += sizeof(Int64)
        when Hash(ShortString, Field)
          size += Table.new(value).bytesize
        when Nil
          size += 0
        else raise Error.new "Unsupported Field type: #{value.class}"
        end
        size
      end

      private def write_field(value, io, format)
        case value
        when Bool
          io.write_byte 't'.ord.to_u8
          io.write_byte(value ? 1_u8 : 0_u8)
        when Int8
          io.write_byte 'b'.ord.to_u8
          io.write_bytes(value, format)
        when UInt8
          io.write_byte 'B'.ord.to_u8
          io.write_byte(value)
        when Int16
          io.write_byte 's'.ord.to_u8
          io.write_bytes(value, format)
        when UInt16
          io.write_byte 'u'.ord.to_u8
          io.write_bytes(value, format)
        when Int32
          io.write_byte 'I'.ord.to_u8
          io.write_bytes(value, format)
        when UInt32
          io.write_byte 'i'.ord.to_u8
          io.write_bytes(value, format)
        when Int64
          io.write_byte 'l'.ord.to_u8
          io.write_bytes(value, format)
        when Float32
          io.write_byte 'f'.ord.to_u8
          io.write_bytes(value, format)
        when Float64
          io.write_byte 'd'.ord.to_u8
          io.write_bytes(value, format)
        when ShortString
          io.write_byte 's'.ord.to_u8
          io.write_bytes value, format
        when String
          io.write_byte 'S'.ord.to_u8
          io.write_bytes LongString.new(value), format
        when Bytes
          io.write_byte 'x'.ord.to_u8
          io.write_bytes(value.bytesize.to_u32, format)
          io.write value
        when Array
          io.write_byte 'A'.ord.to_u8
          size = value.map { |v| Table.field_bytesize(v) }.sum
          io.write_bytes(size.to_u32, format)
          value.each { |v| write_field(v, io, format) }
        when Time
          io.write_byte 'T'.ord.to_u8
          io.write_bytes(value.to_unix.to_i64, format)
        when Hash(ShortString, Field)
          io.write_byte 'F'.ord.to_u8
          io.write_bytes Table.new(value), format
        when Nil
          io.write_byte 'V'.ord.to_u8
        else raise Error.new "Unsupported Field type: #{value.class}"
        end
      end

      private def self.read_field(io, format) : Field
        type = io.read_byte
        case type
        when 't' then io.read_byte == 1_u8
        when 'b' then Int8.from_io(io, format)
        when 'B' then UInt8.from_io(io, format)
        when 's' then Int16.from_io(io, format)
        when 'u' then UInt16.from_io(io, format)
        when 'I' then Int32.from_io(io, format)
        when 'i' then UInt32.from_io(io, format)
        when 'l' then Int64.from_io(io, format)
        when 'f' then Float32.from_io(io, format)
        when 'd' then Float64.from_io(io, format)
        when 'S' then LongString.from_io(io, format)
        when 'x' then read_slice(io, format)
        when 'A' then read_array(io, format)
        when 'T' then Time.unix(Int64.from_io(io, format))
        when 'F' then Table.from_io(io, format)
        when 'V' then nil
        else raise Error.new "Unknown field type '#{type}'"
        end
      end

      private def self.read_array(io, format)
        size = UInt32.from_io(io, format)
        pos = 0_u32
        a = Array(Field).new
        while pos < size
          val = read_field(io, format)
          pos += field_bytesize(val)
          a << val
        end
        a
      end

      private def self.read_slice(io, format)
        size = UInt32.from_io(io, format)
        bytes = Bytes.new(size)
        io.read_fully bytes
        bytes
      end
    end
  end
end
