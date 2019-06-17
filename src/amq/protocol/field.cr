require "./short_string"

module AMQ
  module Protocol
    alias Field = Nil |
                  Bool |
                  Int8 |
                  UInt8 |
                  Int16 |
                  UInt16 |
                  Int32 |
                  UInt32 |
                  Int64 |
                  Float32 |
                  Float64 |
                  ShortString |
                  String |
                  Time |
                  Hash(String, Field) |
                  Array(Field) |
                  Bytes |
                  Array(Hash(String, Field))
  end
end
