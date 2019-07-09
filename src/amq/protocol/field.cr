require "./table"

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
                  String |
                  Time |
                  Table |
                  Hash(String, Field) |
                  Array(Field) |
                  Bytes |
                  Array(Table)
  end
end
