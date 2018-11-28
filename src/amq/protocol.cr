require "./protocol/*"

module AMQ
  module Protocol
    PROTOCOL_START_0_9_1 = UInt8.static_array(65, 77, 81, 80, 0, 0, 9, 1)
    PROTOCOL_START_0_9   = UInt8.static_array(65, 77, 81, 80, 1, 1, 0, 9)
  end
end
