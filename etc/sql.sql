SELECT temp AS t, humidity AS h, location
WHERE temp     >  25.0,
      humidity <  85,
      topic    =~ "home/#"
MODIFY topic  TO "home/summary",
       QoS    TO "QoS2",
       retain TO True,
       temp   TO 20.0
