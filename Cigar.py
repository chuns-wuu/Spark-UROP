from random import randint

def generate_packets():
    packets = [[] for i in range(10)]
    odd_index = randint(0,9)
    for p in range(len(packets)):
        if p != odd_index:
            packets[p] = [10 for i in range(10)]
        else:
            packets[p] = [11 for i in range(10)]
    print packets
    return packets

def weight_once(packets):
    weight = 0
    removed = 0
    for i in range(len(packets)):
        weight+=packets[i][0]*(i+1)
        removed += (i+1)
    # 11*index + 10*(55-index) = weight
    index = weight - 550
    return removed, weight, index

print 'Ten cigar packets: '
packets = generate_packets()
print '(Number of cigars removed, total weight, packet with odd):', weight_once(packets)




