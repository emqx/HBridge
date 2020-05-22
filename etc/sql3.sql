SELECT temp AS t, humidity, location AS l
MODIFY topic TO "home/summary",
       QoS    TO 2,
       retain TO True,
       temp TO 20.0
