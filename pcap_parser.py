# !/usr/bin/env python3
# -*- coding: utf-8 -*-


# =================================================================================================================== #
#                                                    Information                                                      #
#  Author: Hu Hsinpang                                         Email: 1921650867@qq.com                               #
#  Usage: multi-thread scapy-based pcap parser, store parsed result into mysql db_conn                                #
#  requirement: python_3.x, scapy, threading, scapy_http, scapy_ssl_tls, logging, os, pymysql                         #
#  configuration: 128 threads, 1 db_conn                                                                              #
# =================================================================================================================== #

import os
import threading
import glob
try:
    import scapy.all as scapy
except ImportError:
    import scapy
from queue import Queue
import database as db


# =================================================================================================================== #
#                                                 Table of Contents                                                   #
# =================================================================================================================== #

# 1. 单线程函数:定义生产者,消费者功能                  (Line 40 to 74)
# 2. 多线程函数:分配资源,启动多线程                    (Line 82 to 117)
# 3. 主函数:配置文件路径,线程数等                      (Line 124 to 135)


# =================================================================================================================== #
#                                            1. single thread function                                                #
# reference: https://blog.csdn.net/hzliyaya/article/details/52090964                                                  #
#            https://www.cnblogs.com/dplearning/p/8575262.html                                                        #
#            https://scapy.readthedocs.io/en/latest/api/scapy.utils.html                                              #
# =================================================================================================================== #

# producer: single thread pcap parser
def single_thread_pcap_ana(assigned_file_list, out_q, q_writer_lock):
    for file in assigned_file_list:
        try:
            with scapy.rdpcap(file) as packets:
                if packets is None:
                    break
                else:
                    for packet in packets:
                        if packet.haslayer('IP'):
                            value = packet.time, \
                                    packet.src, packet.dst, \
                                    packet["IP"].src, packet["IP"].dst, \
                                    packet.sprintf("%IP.proto%")
                            if packet.haslayer('TCP'):
                                value += packet["TCP"].sport, packet["TCP"].dport
                            elif packet.haslayer('UDP'):
                                value += packet["UDP"].sport, packet["UDP"].dport
                            with q_writer_lock:
                                out_q.put(value)
                        elif packet.haslayer('IPv6'):
                            pass
        except IOError:
            break


# consumer: single thread writer
def single_thread_db_writer(in_q, db_conn, q_reader_lock):
    while not in_q.empty():
        with q_reader_lock:
            seq = in_q.get()
            db_conn.db_table_insert(seq)


# =================================================================================================================== #
#                                             2. multi-thread function                                                #
# matters needing attention: the order of thread create, thread start, and thread join                                #
# =================================================================================================================== #

# multi-thread function, receives file_list, producer_number, and db_connection
def multi_threading(file_list, producer_number, consumer_number, db_conn):
    #  create table in db_conn, in order to store results. create queue and queue_lock
    db_conn.db_table_drop('time_quintuple_tbl')
    db_conn.db_table_create(db.time_quintuple_table)
    time_quintuple_queue, _queue_writer_lock, _queue_reader_lock = \
        Queue(maxsize=0), threading.Lock(), threading.Lock()

    # divide files into producer_number buckets, then distribute them to threads to process
    thread_file_list = [[] for i in range(producer_number)]
    for order in range(len(file_list)):
        thread_file_list[order % producer_number].append(file_list[order])

    # producer: put strings into queue. start producers
    producer_threads_list = []
    for i in range(producer_number):
        producer_thread = threading.Thread(target=single_thread_pcap_ana,
                                           args=(thread_file_list[i], time_quintuple_queue, _queue_writer_lock))
        producer_threads_list.append(producer_thread)
    for i in range(producer_number):
        producer_threads_list[i].start()

    # consumer: get strings from queue and insert them into mysql. start consumers
    consumer_threads_list = []
    for i in range(consumer_number):
        consumer_thread = threading.Thread(target=single_thread_db_writer, 
                                           args=(time_quintuple_queue, db_conn, _queue_reader_lock))
        consumer_threads_list.append(consumer_thread)
    for i in range(consumer_number):
        consumer_threads_list[i].start()

    # block process until threads exit
    for i in range(producer_number):
        producer_threads_list[i].join()
    for i in range(consumer_number):
        consumer_threads_list[i].join()


# =================================================================================================================== #
#                                                    main function                                                    #
# =================================================================================================================== #

# main function entrance
def main():
    file_list = glob.glob('/home/tcpdumpped/*.pcap')

    # create and start multi-thread
    mysql_db = db.DbCon()
    multi_threading(file_list, 120, 6, mysql_db)
    mysql_db.db_close()


if __name__ == '__main__':
    main()
