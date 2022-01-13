for i in range(1, 5):
    f_out = open('community_min_' + str(i * 5) + '.json', 'w')

    count = 0
    with open('pairs.json', 'r') as f:
        for line in f:
            authors = line.strip().split('\t')
            if len(authors) >= i * 5:
                count += 1
                f_out.writelines(line)

    print(f'total community: {count}, min {i * 5}')
    f_out.close()
