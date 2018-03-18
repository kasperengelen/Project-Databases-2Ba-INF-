for line in open('dataset_utils.py', 'r'):
    if 'redirect' in line:
        split = line.split("'")
        if len(split) < 2:
            continue
        else:
            print(split[1])
