import argparse
import socket
import sys
import time
from collections import deque

from utils import PacketHeader, compute_checksum

MAX_PACKET_SIZE = 1472 
HEADER_SIZE = 16       
DATA_SIZE = MAX_PACKET_SIZE - HEADER_SIZE  

START_TYPE = 0
END_TYPE   = 1
DATA_TYPE  = 2
ACK_TYPE   = 3

TIMEOUT = 0.5  

def chunk_data(data, chunk_size):
    """Chia data thành các mảnh (chunk) có kích cỡ chunk_size."""
    for i in range(0, len(data), chunk_size):
        yield data[i:i+chunk_size]

def create_packet(pkt_type, seq_num, payload=b""):
    """Tạo gói (header + payload) và tính checksum."""
    header = PacketHeader(type=pkt_type, seq_num=seq_num, length=len(payload), checksum=0)
    
    raw = bytes(header) + payload
    chksum = compute_checksum(raw)
    header.checksum = chksum
    
    pkt = bytes(header) + payload
    return pkt

def sender(receiver_ip, receiver_port, window_size):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.settimeout(0.01)  
    data = sys.stdin.buffer.read()  

    start_pkt = create_packet(START_TYPE, 0, b"")
    s.sendto(start_pkt, (receiver_ip, receiver_port))

    start_acked = False
    while not start_acked:
        try:
            rcv, _ = s.recvfrom(2048)
            header = PacketHeader(rcv[:HEADER_SIZE])
            if header.type == ACK_TYPE and header.seq_num == 1:

                start_acked = True

        except socket.timeout:

            s.sendto(start_pkt, (receiver_ip, receiver_port))


    data_chunks = list(chunk_data(data, DATA_SIZE))
    total_chunks = len(data_chunks)

    base = 1               
    next_seq = 1           
    window = deque()       
    last_ack_time = time.time()

    done_sending = False
    while not done_sending:

        while next_seq - base < window_size and (next_seq - 1) < total_chunks:
            payload = data_chunks[next_seq - 1]
            pkt = create_packet(DATA_TYPE, next_seq, payload)
            s.sendto(pkt, (receiver_ip, receiver_port))
            window.append((next_seq, pkt))
            next_seq += 1
            last_ack_time = time.time()

        updated = False
        while True:
            try:
                rcv, _ = s.recvfrom(2048)
                header = PacketHeader(rcv[:HEADER_SIZE])
                if header.type == ACK_TYPE:
                    if header.seq_num > base:
                        base = header.seq_num
                        updated = True
         
                        while window and window[0][0] < base:
                            window.popleft()
            except socket.timeout:
                break

        if updated:
            last_ack_time = time.time()

        if (time.time() - last_ack_time) > TIMEOUT and window:
            for (seqi, pkt) in window:
                s.sendto(pkt, (receiver_ip, receiver_port))
            last_ack_time = time.time()

        if base > total_chunks:
            done_sending = True

    end_seq = total_chunks + 1
    end_pkt = create_packet(END_TYPE, end_seq, b"")
    s.sendto(end_pkt, (receiver_ip, receiver_port))
    end_start_time = time.time()
    end_acked = False
    while True:
        if (time.time() - end_start_time) > TIMEOUT:

            break
        try:
            rcv, _ = s.recvfrom(2048)
            header = PacketHeader(rcv[:HEADER_SIZE])
            if header.type == ACK_TYPE and header.seq_num == end_seq + 1:
                end_acked = True
                break
        except socket.timeout:
            pass
    # Xong
    s.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("receiver_ip", help="The IP address of the host that receiver is running on")
    parser.add_argument("receiver_port", type=int, help="The port number on which receiver is listening")
    parser.add_argument("window_size", type=int, help="Maximum number of outstanding packets")
    args = parser.parse_args()

    sender(args.receiver_ip, args.receiver_port, args.window_size)


if __name__ == "__main__":
    main()
