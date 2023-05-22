import sys

def main():
    out_file = sys.argv[1]
    all_kvs = {}
    for file_name in sys.argv[2:]:
        f = open(file_name, "r")
        while line := f.readline():
            s = line.rstrip()
            s_toks = s.split(": ")
            k, v = s[0], s[1]
            
    

if __name__ == "__main__":
    main()
