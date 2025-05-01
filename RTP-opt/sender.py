import argparse
import socket
import sys
import time

from collections import OrderedDict
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
    for i in range(0, len(data), chunk_size):
        yield data[i:i+chunk_size]

def create_packet(pkt_type, seq_num, payload=b""):
    header = PacketHeader(type=pkt_type, seq_num=seq_num, length=len(payload), checksum=0)
    raw = bytes(header) + payload
    chksum = compute_checksum(raw)
    header.checksum = chksum
    return bytes(header) + payload

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
    window = OrderedDict()
    last_ack_time = time.time()

    done_sending = False

    while not done_sending:
        while next_seq - base < window_size and (next_seq - 1) < total_chunks:
            payload = data_chunks[next_seq - 1]
            pkt = create_packet(DATA_TYPE, next_seq, payload)
            window[next_seq] = [pkt, False] 
            s.sendto(pkt, (receiver_ip, receiver_port))
            next_seq += 1
            last_ack_time = time.time()


        updated = False
        while True:
            try:
                rcv, _ = s.recvfrom(2048)
                header = PacketHeader(rcv[:HEADER_SIZE])
                if header.type == ACK_TYPE:
                    ack_seq = header.seq_num
                    if ack_seq in window and not window[ack_seq][1]:
                        window[ack_seq][1] = True 
                        updated = True

            except socket.timeout:
                break

        if updated:
            last_ack_time = time.time()
            while window and list(window.items())[0][1][1] is True:
                first_key = list(window.keys())[0]
                del window[first_key]
                base += 1

        if (time.time() - last_ack_time) > TIMEOUT and window:
            for seqi, (pkt_bytes, acked) in window.items():
                if not acked:
                    s.sendto(pkt_bytes, (receiver_ip, receiver_port))
            last_ack_time = time.time()

        if base > total_chunks:
            done_sending = True

    end_seq = total_chunks + 1
    end_pkt = create_packet(END_TYPE, end_seq, b"")
    s.sendto(end_pkt, (receiver_ip, receiver_port))
    end_start_time = time.time()

    while True:
        if (time.time() - end_start_time) > TIMEOUT:
            break
        try:
            rcv, _ = s.recvfrom(2048)
            header = PacketHeader(rcv[:HEADER_SIZE])
            if header.type == ACK_TYPE and header.seq_num == end_seq + 1:
                break
        except socket.timeout:
            pass

    s.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("receiver_ip", help="receiver ip")
    parser.add_argument("receiver_port", type=int)
    parser.add_argument("window_size", type=int)
    args = parser.parse_args()
    sender(args.receiver_ip, args.receiver_port, args.window_size)

if __name__ == "__main__":
    main()
