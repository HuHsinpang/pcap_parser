# pcap_parser
multithreading scapy based pcap parser. get time, mac_src, mac_dst, ip_src, ip_dst, ip_proto, sport, dport from pacp, then store them into mysql

# pcap解析
- 一个基于python的多线程pcap解析工具。程序调用了scapy的函数进行pcap包解析，现阶段只解析了每一帧的time, mac_src, mac_dst, ip_src, ip_dst, ip_proto, sport, dport, 后续会加入对其他部分的解析。
- 每个解析线程（生产者）解析pcap时获取到这些字段后，存入queue队列。多个存程序（消费者）从queue中读取数据，存入mysql数据库。这里的生产者和消费者是从_队列_的角度来看的 
- 存在疑惑：对python中queue与mysql存的原理理解不透彻，程序中的锁是否有必要加？
