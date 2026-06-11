require "json/builder"
require "base64"
require "./field"
require "./errors"

module AMQ
  module Protocol
    class Table
      BYTEFORMAT = IO::ByteFormat::NetworkEndian
      @buffer : Pointer(UInt8)
      @capacity = 0
      @bytesize = 0
      @read_only = false

      def initialize(hash : Hash(String, Field) | NamedTuple)
        hash.each { |k, v| @capacity += 1 + k.to_s.bytesize + 1 + capacity_required(v) }
        if @capacity.zero?
          @buffer = Pointer(UInt8).null
        else
          @buffer = GC.malloc_atomic(@capacity).as(UInt8*)
        end
        hash.each do |key, value|
          write_short_string(key.to_s)
          write_field(value)
        end
      end

      def initialize(bytes : Bytes? = Bytes.empty)
        bytes ||= Bytes.empty
        @buffer = bytes.to_unsafe
        @bytesize = @capacity = bytes.bytesize
        @read_only = bytes.read_only?
      end

      def clone
        Table.new to_slice.clone
      end

      def []?(key : String)
        fetch(key) { nil }
      end

      def [](key : String)
        fetch(key) { raise KeyError.new "Missing hash key: #{key.inspect}" }
      end

      def fetch(key : String, default : Field)
        fetch(key) { default }
      end

      def fetch(key : String, &)
        with_pos_loop do |pos|
          if key_matches?(pos, key.to_slice)
            return read_field(pos)
          else
            skip_field(pos)
          end
        end
        yield
      end

      def has_key?(key : String) : Bool
        with_pos_loop do |pos|
          if key_matches?(pos, key.to_slice)
            return true
          else
            skip_field(pos)
          end
        end
        false
      end

      @[Deprecated("key must be String")]
      def has_key?(key)
        has_key?(key.to_s)
      end

      # Returns true if the table contains *key* and its value equals *value*.
      # Semantically identical to `has_key?(key) && self[key] == value` but
      # does not allocate for scalar and string values.
      def has_entry?(key : String, value : Field) : Bool
        with_pos_loop do |pos|
          if key_matches?(pos, key.to_slice)
            return field_equals?(pos, value)
          else
            skip_field(pos)
          end
        end
        false
      end

      # Compares the field at *pos* with *value* without materializing a
      # String when both are strings; other types fall back to `read_field`.
      private def field_equals?(pos : Int32*, value : Field) : Bool
        if value.is_a?(String)
          ensure_available(pos.value, 1)
          if 'S' === @buffer[pos.value]
            pos.value += 1
            sz = BYTEFORMAT.decode(UInt32, to_slice(pos.value, sizeof(UInt32)))
            pos.value += sizeof(UInt32)
            stored = to_slice(pos.value, sz)
            pos.value += sz
            return value.to_slice == stored
          end
        end
        read_field(pos) == value
      end

      def each(& : (String, Field) -> _)
        with_pos_loop do |pos|
          k = read_short_string(pos)
          v = read_field(pos)
          yield k, v
        end
      end

      def each_key(& : String -> _)
        with_pos_loop do |pos|
          k = read_short_string(pos)
          skip_field(pos)
          yield k
        end
      end

      def size
        i = 0
        with_pos_loop do |pos|
          skip_short_string(pos)
          skip_field(pos)
          i += 1
        end
        i
      end

      def any?(& : (String, Field) -> _) : Bool
        each do |k, v|
          return true if yield(k, v)
        end
        false
      end

      def all?(& : (String, Field) -> _) : Bool
        each do |k, v|
          return false unless yield(k, v)
        end
        true
      end

      def empty?
        @bytesize.zero?
      end

      def []=(key : String, value : Field)
        check_writeable
        unless overwrite_or_delete(key, value)
          write_short_string(key)
          write_field(value)
        end
        value
      end

      # If `key` exists with a fixed-size value of the same type, overwrite the
      # value bytes in place and return true. Otherwise remove any existing
      # entry and return false so the caller can append the new key/value.
      private def overwrite_or_delete(key : String, value : Field) : Bool
        new_prefix = fixed_size_prefix(value)
        pos = 0
        while pos < @bytesize
          kv_start = pos
          if key_matches?(pointerof(pos), key.to_slice)
            if new_prefix && @buffer[pos] == new_prefix
              overwrite_fixed_value_at(value, pos + 1)
              return true
            end
            skip_field(pointerof(pos))
            kv_length = pos - kv_start
            (@buffer + kv_start).move_from(@buffer + pos, @bytesize - pos)
            @bytesize -= kv_length
            return false
          else
            skip_field(pointerof(pos))
          end
        end
        false
      end

      private def fixed_size_prefix(value) : UInt8?
        case value
        when Int, Float32, Float64, Bool, Time, Nil then prefix(value).ord.to_u8
        end
      end

      private def overwrite_fixed_value_at(value, pos : Int32) : Nil
        case value
        when Int, Float32, Float64
          BYTEFORMAT.encode(value, Slice.new(@buffer + pos, sizeof_num(value)))
        when Bool
          @buffer[pos] = value ? 1_u8 : 0_u8
        when Time
          BYTEFORMAT.encode(value.to_unix.to_i64, Slice.new(@buffer + pos, sizeof(Int64)))
        when Nil
          # no value bytes
        end
      end

      def to_h
        h = Hash(String, Field).new
        with_pos_loop do |pos|
          k = read_short_string(pos)
          h[k] = read_field(pos, table_to_h: true)
        end
        h
      end

      def to_json(json : JSON::Builder)
        json.object do
          with_pos_loop do |pos|
            key = read_short_string(pos)
            value = read_field(pos)
            json.field key do
              value_to_json(value, json)
            end
          end
        end
      end

      private def value_to_json(v, json)
        case v
        when Bytes
          json.string Base64.encode(v)
        when Array
          json.array do
            v.each { |e| value_to_json(e, json) }
          end
        when Hash, NamedTuple
          json.object do
            v.each do |k, val|
              json.field k.to_s do
                value_to_json(val, json)
              end
            end
          end
        else
          v.to_json(json)
        end
      end

      def inspect(io)
        io << {{ @type.name.id.stringify }} << '('
        first = true
        with_pos_loop do |pos|
          io << ", " unless first
          io << '@' << read_short_string(pos)
          io << '='
          read_field(pos).inspect(io)
          first = false
        end
        io << ')'
      end

      # Comparition on a semantic level, not on byte level
      def ==(other : self)
        return false if size != other.size
        each do |k, v|
          return false if other[k] != v
        rescue KeyError
          return false
        end
        true
      end

      # See `Object#hash(hasher)`
      def hash(hasher)
        # Copied from Hash#hash
        # The hash value must be the same regardless of the
        # order of the keys.
        result = hasher.result

        each do |key, value|
          copy = hasher
          copy = key.hash(copy)
          copy = value.hash(copy)
          result &+= copy.result
        end

        result.hash(hasher)
      end

      def delete(key : String)
        check_writeable
        do_delete(key)
      end

      private def do_delete(key : String)
        pos = 0
        while pos < @bytesize
          start_pos = pos
          if key_matches?(pointerof(pos), key.to_slice)
            v = read_field(pointerof(pos))
            kv_length = pos - start_pos
            (@buffer + start_pos).move_from(@buffer + pos, @bytesize - pos)
            @bytesize -= kv_length
            return v
          else
            skip_field(pointerof(pos))
          end
        end
        nil
      end

      @[Deprecated("key must be String")]
      def delete(key)
        delete(key.to_s)
      end

      def to_io(io, format) : Nil
        io.write_bytes(@bytesize.to_u32, format)
        io.write to_slice
      end

      def self.from_bytes(bytes, format = BYTEFORMAT) : self
        raise IO::EOFError.new("Unexpected EOF while reading table size") if bytes.size < sizeof(UInt32)
        size = format.decode(UInt32, bytes[0, 4])
        if size > bytes.size - sizeof(UInt32)
          raise IO::EOFError.new("Unexpected EOF while reading table")
        end
        if bytes.read_only?
          slice = bytes[4, size]
        else
          slice = Bytes.new(size, read_only: false)
          slice.copy_from(bytes.to_unsafe + 4, size)
        end
        new(slice)
      end

      def self.from_io(io, format, size : UInt32? = nil) : self
        size ||= UInt32.from_io(io, format)
        case io
        when IO::Memory
          if io.@writeable # Need to copy the bytes to prevent modification being mirrored here
            bytes = Bytes.new(size)
            io.read_fully(bytes)
            new(bytes)
          else
            if size > io.bytesize - io.pos
              raise IO::EOFError.new("Unexpected EOF while reading table")
            end
            bytes = Bytes.new(io.buffer + io.pos, size, read_only: true)
            io.pos += size
            new(bytes)
          end
        else
          if stream = io.as?(AMQ::Protocol::Stream)
            stream.assert_within_frame(size)
          end
          buffer = Bytes.new(size)
          io.read_fully(buffer)
          new(buffer)
        end
      end

      def bytesize
        sizeof(UInt32) + @bytesize
      end

      def to_slice(pos = 0, length = @bytesize - pos)
        ensure_available(pos, length)
        Bytes.new(@buffer + pos, length, read_only: @read_only)
      end

      def reject!(& : String, Field -> _) : self
        check_writeable
        pos = 0
        while pos < @bytesize
          start_pos = pos
          key = read_short_string(pointerof(pos))
          value = read_field(pointerof(pos))
          if yield key, value
            kv_length = pos - start_pos
            (@buffer + start_pos).move_from(@buffer + pos, @bytesize - pos)
            @bytesize -= kv_length
            pos -= kv_length
          end
        end
        self
      end

      def merge!(other : Hash(String, Field) | NamedTuple | self) : self
        return self if other.is_a?(Table) && other.same?(self)

        check_writeable
        other.each_key { |k| do_delete(k.to_s) }
        capacity = 0
        other.each { |k, v| capacity += 1 + k.to_s.bytesize + 1 + capacity_required(v) }
        ensure_capacity(capacity)
        other.each do |key, value|
          write_short_string(key.to_s)
          write_field(value)
        end
        self
      end

      # Iterates the table with a thread-local `pos`, yielding a pointer to it
      # for the read helpers to advance. Keeping `pos` local (not an instance
      # variable) is what makes concurrent iteration safe without locking.
      private def with_pos_loop(&)
        pos = 0
        while pos < @bytesize
          yield pointerof(pos)
        end
      end

      private def ensure_available(pos : Int32, length : Int) : Nil
        return if pos >= 0 && pos <= @bytesize && length >= 0 && length <= @bytesize - pos

        raise IO::EOFError.new("Unexpected EOF while reading table")
      end

      private def check_writeable : Nil
        return unless @read_only
        buffer = GC.malloc_atomic(@bytesize).as(UInt8*)
        buffer.copy_from(@buffer, @bytesize)
        @capacity = @bytesize
        @read_only = false
        @buffer = buffer
      end

      private def write_short_string(str : String)
        ensure_capacity(1 + str.bytesize)
        @buffer[@bytesize] = str.bytesize.to_u8; @bytesize += 1
        str.to_slice.copy_to(@buffer + @bytesize, str.bytesize)
        @bytesize += str.bytesize
        str
      end

      private def write_field(value) : Nil
        case value
        when JSON::Any
          write_field(value.raw)
        when Hash, NamedTuple
          write_field(Table.new(value))
        when Int, Float32, Float64
          ensure_capacity(1 + sizeof_num(value))
          write_prefix(value)
          write_num(value)
        when Bool
          ensure_capacity(1 + sizeof(Bool))
          write_prefix(value)
          @buffer[@bytesize] = value ? 1_u8 : 0_u8
          @bytesize += 1
        when String
          ensure_capacity(1 + sizeof(UInt32) + value.bytesize)
          write_prefix(value)
          write_num(value.bytesize.to_u32)
          value.to_slice.copy_to(@buffer + @bytesize, value.bytesize)
          @bytesize += value.bytesize
        when Bytes
          ensure_capacity(1 + sizeof(UInt32) + value.size)
          write_prefix(value)
          write_num(value.size.to_u32)
          value.copy_to(@buffer + @bytesize, value.size)
          @bytesize += value.size
        when Array
          ensure_capacity(1 + sizeof(UInt32))
          write_prefix(value)
          length_pos = @bytesize
          write_num(0u32) # update size later
          start_pos = @bytesize
          value.each { |v| write_field(v) }
          end_pos = @bytesize
          array_bytesize = end_pos - start_pos
          BYTEFORMAT.encode(array_bytesize.to_u32, Slice.new(@buffer + length_pos, sizeof(UInt32)))
        when Time
          ensure_capacity(1 + sizeof(Int64))
          write_prefix(value)
          write_num(value.to_unix.to_i64)
        when Table
          tbl_slice = value.to_slice
          ensure_capacity(1 + sizeof(UInt32) + tbl_slice.size)
          write_prefix(value)
          write_num(tbl_slice.size.to_u32)
          tbl_slice.copy_to(@buffer + @bytesize, tbl_slice.size)
          @bytesize += tbl_slice.size
        when Nil
          ensure_capacity(1)
          write_prefix(value)
        else raise Error.new "Unsupported Field type: #{value.class}"
        end
      end

      private def ensure_capacity(size : Int)
        required_capacity = @bytesize + size
        if required_capacity > @capacity
          capacity = Math.pw2ceil(required_capacity)
          @buffer = GC.realloc(@buffer, capacity)
          @capacity = capacity
        end
      end

      private def write_prefix(value)
        @buffer[@bytesize] = prefix(value).ord.to_u8
        @bytesize += 1
      end

      private def prefix(value) : Char
        case value
        when Int8    then 'b'
        when UInt8   then 'B'
        when Int16   then 's'
        when UInt16  then 'u'
        when Int32   then 'I'
        when UInt32  then 'i'
        when Int64   then 'l'
        when Float32 then 'f'
        when Float64 then 'd'
        when Bool    then 't'
        when String  then 'S'
        when Bytes   then 'x'
        when Array   then 'A'
        when Time    then 'T'
        when Table   then 'F'
        when Nil     then 'V'
        else              raise "Unexpected field type #{value.class}"
        end
      end

      private def write_num(value) : Nil
        size = sizeof_num(value)
        BYTEFORMAT.encode(value, Slice.new(@buffer + @bytesize, size))
        @bytesize += size
      end

      private def sizeof_num(value) : Int32
        case value
        when Int8, UInt8   then 1
        when Int16, UInt16 then 2
        when Int32, UInt32 then 4
        when Int64, UInt64 then 8
        when Float32       then 4
        when Float64       then 8
        else                    raise "unexpected value type #{value.class}"
        end
      end

      private def skip_field(pos : Int32*) : Nil
        ensure_available(pos.value, 1)
        type = @buffer[pos.value]; pos.value += 1
        pos.value += sizeof_field_type(type, pos)
      end

      private def sizeof_field_type(type, pos : Int32*) : Int32
        size = case type
               when 't' then sizeof(UInt8)
               when 'b' then sizeof(Int8)
               when 'B' then sizeof(UInt8)
               when 's' then sizeof(Int16)
               when 'u' then sizeof(UInt16)
               when 'U' then sizeof(Int16) # AMQP 0-9 compatibility
               when 'I' then sizeof(Int32)
               when 'i' then sizeof(UInt32)
               when 'l' then sizeof(Int64)
               when 'L' then sizeof(Int64) # AMQP 0-9 compatibility
               when 'f' then sizeof(Float32)
               when 'd' then sizeof(Float64)
               when 'D' then 1 + sizeof(Int32)
               when 'T' then sizeof(Int64)
               when 'S', 'x', 'A', 'F'
                 sizeof(UInt32) + BYTEFORMAT.decode(UInt32, to_slice(pos.value, sizeof(UInt32)))
               when 'V' then 0
               else          raise Error.new "Unknown field type '#{type}'"
               end
        ensure_available(pos.value, size)
        size
      end

      private def capacity_required(value) : Int32
        case value
        when Hash
          sizeof(UInt32) + value.each.sum { |k, v| 1 + k.bytesize + 1 + capacity_required(v) }
        when NamedTuple
          required = sizeof(UInt32)
          value.each { |k, v| required += 1 + k.to_s.bytesize + 1 + capacity_required(v) }
          required
        when JSON::Any             then capacity_required(value.raw)
        when Int, Float32, Float64 then sizeof_num(value)
        when Bool                  then sizeof(Bool)
        when String                then sizeof(UInt32) + value.bytesize
        when Bytes                 then sizeof(UInt32) + value.size
        when Array                 then sizeof(UInt32) + value.sum { |v| 1 + capacity_required(v) }
        when Time                  then sizeof(Int64)
        when Table                 then value.bytesize
        when Nil                   then 0
        else                            raise Error.new "Unsupported Field type: #{value.class}"
        end
      end

      private def key_matches?(pos : Int32*, key_bytes : Bytes) : Bool
        ensure_available(pos.value, 1)
        sz = @buffer[pos.value]
        pos.value += 1
        local_key_bytes = to_slice(pos.value, sz)
        pos.value += sz
        key_bytes == local_key_bytes
      end

      private def skip_short_string(pos : Int32*)
        ensure_available(pos.value, 1)
        size = @buffer[pos.value]
        ensure_available(pos.value + 1, size)
        pos.value += 1 + size
      end

      private def read_short_string(pos : Int32*)
        ensure_available(pos.value, 1)
        sz = @buffer[pos.value]
        pos.value += 1
        ensure_available(pos.value, sz)
        str = String.new(@buffer + pos.value, sz)
        pos.value += sz
        str
      end

      private def read_field(pos : Int32*, table_to_h = false) : Field
        ensure_available(pos.value, 1)
        type = @buffer[pos.value]; pos.value += 1
        case type
        when 't' then read_bool(pos)
        when 'b' then read_num(Int8, pos)
        when 'B' then read_num(UInt8, pos)
        when 's' then read_num(Int16, pos)
        when 'u' then read_num(UInt16, pos)
        when 'U' then read_num(Int16, pos) # AMQP 0-9 compatibility
        when 'I' then read_num(Int32, pos)
        when 'i' then read_num(UInt32, pos)
        when 'l' then read_num(Int64, pos)
        when 'L' then read_num(Int64, pos) # AMQP 0-9 compatibility
        when 'f' then read_num(Float32, pos)
        when 'd' then read_num(Float64, pos)
        when 'S' then read_long_string(pos)
        when 'x' then read_slice(pos)
        when 'A' then read_array(pos, table_to_h)
        when 'T' then read_time(pos)
        when 'F' then read_table(pos, table_to_h)
        when 'D' then read_decimal(pos)
        when 'V' then nil
        else          raise Error.new "Unknown field type '#{type}'"
        end
      end

      private def read_bool(pos : Int32*) : Bool
        ensure_available(pos.value, sizeof(UInt8))
        v = @buffer[pos.value] == 1_u8
        pos.value += 1
        v
      end

      private def read_num(t : T.class, pos : Int32*) : T forall T
        v = BYTEFORMAT.decode(t, to_slice(pos.value, sizeof(T)))
        pos.value += sizeof(T)
        v
      end

      private def read_time(pos : Int32*) : Time
        Time.unix(read_num(Int64, pos))
      end

      private def read_table(pos : Int32*, table_to_h) : Table | Hash(String, Field)
        size = BYTEFORMAT.decode(UInt32, to_slice(pos.value, sizeof(UInt32)))
        t = Table.from_bytes(to_slice(pos.value, sizeof(UInt32) + size))
        pos.value += sizeof(UInt32) + size
        table_to_h ? t.to_h : t
      end

      private def read_long_string(pos : Int32*) : String
        sz = BYTEFORMAT.decode(UInt32, to_slice(pos.value, sizeof(UInt32)))
        pos.value += sizeof(UInt32)
        str = String.new(to_slice(pos.value, sz))
        pos.value += sz
        str
      end

      private def read_decimal(pos : Int32*) : Float64
        ensure_available(pos.value, 1 + sizeof(Int32))
        scale = @buffer[pos.value]
        pos.value += 1
        value = BYTEFORMAT.decode(Int32, to_slice(pos.value, sizeof(Int32)))
        pos.value += sizeof(Int32)
        value / 10**scale
      end

      private def read_array(pos : Int32*, table_to_h = false)
        size = BYTEFORMAT.decode(UInt32, to_slice(pos.value, sizeof(UInt32)))
        pos.value += sizeof(UInt32)
        ensure_available(pos.value, size)
        end_pos = pos.value + size
        a = Array(Field).new
        while pos.value < end_pos
          a << read_field(pos, table_to_h)
        end
        a
      end

      private def read_slice(pos : Int32*) : Bytes
        size = BYTEFORMAT.decode(UInt32, to_slice(pos.value, sizeof(UInt32)))
        pos.value += sizeof(UInt32)
        bytes = to_slice(pos.value, size).dup
        pos.value += size
        bytes
      end
    end
  end
end
