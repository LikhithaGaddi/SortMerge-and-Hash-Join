import math
import os
import glob
from pathlib import Path
import heapq
import sys
import time
start_time = time.time()



R_file = sys.argv[1]
S_file = sys.argv[2]
join_type = sys.argv[3]
mm_size = int(sys.argv[4])


op = R_file.split("/")[-1]+"_"+S_file.split("/")[-1]+"_join.txt"

no_of_tuples_per_block = 100

tuples = no_of_tuples_per_block * mm_size


class Merge(object):
    def __init__(self, val):
        self.val = val

    def __lt__(self, other):
        return self.cmp(self.val, other.val)

    def cmp(self, l1, l2):
        for i in range(0, len(l1)):
            if(l1[i] < l2[i]):
                return l1 < l2


class SortMergejoin(object):
    def __init__(self, R_file, S_file, tuples, no_of_tuples_per_block):
        self.R_file = R_file
        self.S_file = S_file
        self.tuples = tuples
        self.no_of_tuples_per_block = no_of_tuples_per_block
        self.hr = []
        self.hs = []

    def get_column(self, line):
        return line.split(" ")

    def heap_sort(self, columns_of_list, col):
        index = 0
        h = []
        return_value = []
        for i in range(len(columns_of_list)):
            temp = []
            temp.append(columns_of_list[i][col])
            temp.append(index)
            index += 1
            heapq.heappush(h, temp)

        for i in range(len(columns_of_list)):
            x = heapq.heappop(h)
            return_value.append(columns_of_list[x[-1]])

        return return_value

    def sort_helper(self, filename, col):
        try:
            fp = open(filename, "r")
        except OSError:
            print("Could not open/read file:", filename)
            sys.exit()
        
        size = Path(filename).stat().st_size
        size_of_each_tuple = len(fp.readline())
        fp.close()
        fp = open(filename, "r")
        size = math.ceil(size / size_of_each_tuple)
        no_of_sublists = math.ceil(size / self.tuples)
        columns_of_list = []
        index = 0
        x = 0
        for line in fp:
            if x == self.tuples:
                name = filename.split(
                    "/")[-1].split(".")[0] + "_" + str(index) + ".txt"
                index += 1
                with open(name, "w") as f:
                    written_list = []
                    columns_of_list = self.heap_sort(columns_of_list, col)
                    for i in range(len(columns_of_list)):
                        columns_of_list[i] = " ".join(columns_of_list[i])
                        written_list.append(columns_of_list[i])
                    f.writelines(written_list)
                x = 0
                columns_of_list = []
            columns_of_list.append(self.get_column(line))
            x += 1

        if x != self.tuples or len(columns_of_list) != 0:
            name = filename.split("/")[-1].split(".")[0] + \
                "_" + str(index) + ".txt"
            with open(name, "w") as f:
                written_list = []
                columns_of_list = self.heap_sort(columns_of_list, col)
                for i in range(len(columns_of_list)):
                    columns_of_list[i] = "  ".join(columns_of_list[i])
                    written_list.append(columns_of_list[i])
                f.writelines(written_list)
            index += 1

        fp.close()

        fp = []

        no_of_sublists = index

        for i in range(0, (no_of_sublists)):
            f = filename.split(
                "/")[-1].split(".")[0] + "_" + str(i) + ".txt"
            x = open(f, "r")
            fp.append(x)

        return no_of_sublists, fp

    def push_to_hr_helper(self, line, file_no, line_no):
        cols = line.split(" ")
        if (cols[1] == "" and len(cols) > 2):
            cols[1] = cols[2]
        temp = [cols[1][:-1], cols[0], line_no, file_no]
        heapq.heappush(self.hr, Merge(temp))

    def push_to_hs_helper(self, line, file_no, line_no):
        cols = line.split(" ")
        if (cols[1] == "" and len(cols) > 2):
            cols[1] = cols[2]
        temp = [cols[0], cols[1], line_no, file_no]
        heapq.heappush(self.hs, Merge(temp))

    def push_to_hr(self, fpr, i):
        for j in range(0, self.no_of_tuples_per_block):
            s = fpr[i].readline()
            if len(s) > 0:
                self.push_to_hr_helper(s, i, j+1)
            else:
                break

    def push_to_hs(self, fps, i):
        for j in range(0, self.no_of_tuples_per_block):
            s = fps[i].readline()
            if len(s) > 0:
                self.push_to_hs_helper(s, i, j+1)
            else:
                break

    def sort_files(self, delete_files=False):
        r_sublists, fpr = self.sort_helper(self.R_file, 1)
        s_sublists, fps = self.sort_helper(self.S_file, 0)

        f = open(op, "w")
        if (r_sublists + s_sublists >= mm_size):
            self.close_files(fpr, fps, f)
            print("Number of files ar more than the blocks in main memory")
            exit(0)

        for i in range(r_sublists):
            self.push_to_hr(fpr, i)

        for i in range(s_sublists):
            self.push_to_hs(fps, i)

        temp = []
        v = None

        while len(self.hs) != 0 and len(self.hr) != 0:
            pr = heapq.heappop(self.hr).val
            if (v != pr[0]):
                temp = []
                v = pr[0]
                if (pr[2] % self.no_of_tuples_per_block == 0 and pr[2] != 0):
                    self.push_to_hr(fpr, pr[3])

                while (len(self.hs) != 0):
                    ps = heapq.heappop(self.hs).val
                    if (ps[2] % self.no_of_tuples_per_block == 0 and ps[2] != 0):
                        self.push_to_hs(fps, ps[3])
                    if(pr[0] == ps[0]):
                        temp.append(ps)
                    elif (pr[0] > ps[0]):
                        continue
                    else:
                        heapq.heappush(self.hs, Merge(ps))
                        break

            if (len(temp) > 0):
                for i in range(len(temp)):
                    t = " ".join([pr[1], pr[0], temp[i][1]])
                    f.writelines(t)

        if (delete_files):
            pass

        return fpr, fps, f

    def close_files(self, fpr, fps, f):
        l = len(fpr)
        for i in range(0, l):
            os.remove(fpr[i].name)

        l = len(fps)
        for i in range(0, l):
            os.remove(fps[i].name)
        f.close()


class HashJoin(object):

    def __init__(self, R_file, S_file, no_of_tuples_per_block):
        self.R_file = R_file
        self.S_file = S_file
        self.no_of_tuples_per_block = no_of_tuples_per_block

    def cal_hash(self, s):
        ans = 0
        for i in range(len(s)):
            ans += ord(s[i])
        return ans % (mm_size-1)

    def open_files(self, s):
        fp = []
        for i in range(mm_size-1):
            temp = s+str(i)+".txt"
            f = open(temp, "w+")
            fp.append(f)
        return fp

    def join_files(self, fpr, fps):        
        try:
            f = open(self.R_file, "r")
        except OSError:
            print("Could not open/read file:", self.R_file)
            self.close_files(fpr, fps)
            sys.exit()

        dic = {}

        for i in range(mm_size-1):
            dic[i] = []

        while (1):
            line = f.readline()
            if (len(line) == 0):
                break

            h = self.cal_hash(line.split(" ")[1][:-1])
            dic[h].append(line)
            if(len(dic[h]) == self.no_of_tuples_per_block):
                fpr[h].write(''.join(dic[h]))
                dic[h] = []

        for i in range(mm_size-1):
            if (len(dic[i]) != 0):
                fpr[i].write(''.join(dic[i]))
                dic[i] = []

        f.close()


        try:
            f = open(self.S_file, "r")
        except OSError:
            print("Could not open/read file:", self.S_file)
            self.close_files(fpr, fps)
            sys.exit()

        while (1):
            line = f.readline()
            if (len(line) == 0):
                break

            h = self.cal_hash(line.split(" ")[0])
            dic[h].append(line)
            if(len(dic[h]) == self.no_of_tuples_per_block):
                fps[h].write(''.join(dic[h]))
                dic[h] = []

        for i in range(mm_size-1):
            if (len(dic[i]) != 0):
                fps[i].write(''.join(dic[i]))
                dic[i] = []

        f.close()

        l = len(fpr)
        for i in range(0, l):
            fpr[i].close()
            fpr[i] = open("R"+str(i)+".txt", "r")

        l = len(fps)
        for i in range(0, l):
            fps[i].close()
            fps[i] = open("S"+str(i)+".txt", "r")

        f = open(op, "w")

        for i in range(mm_size-1):
            s = "R"+str(i)+".txt"
            r_size = os.path.getsize(s)
            s = "S"+str(i)+".txt"
            s_size = os.path.getsize(s)

            if r_size == 0 or s_size == 0:
                continue

            l = []

            if r_size <= s_size:
                # print("r<s")
                fp = open("R" + str(i)+".txt")
                Lines = fp.readlines()

                if (len(Lines) > no_of_tuples_per_block * mm_size):
                    self.close_files(fpr, fps, f)
                    print("Size of sublist is more than main memory")
                    exit(0)

                for line in Lines:
                    l.append(line.split(" "))

                fp.close()

                fs = open("S" + str(i)+".txt")

                temp = []
                while True:
                    p = fs.readline()
                    if (len(p) == 0):
                        break
                    p = p.split(" ")
                    for j in range(len(l)):
                        if p[0] == l[j][1][:-1]:
                            s = str(l[j][0]) + " " + \
                                str(l[j][1][:-1]) + " " + str(p[1])
                            temp.append(s)
                            if (len(temp) == self.no_of_tuples_per_block):
                                f.write(''.join(temp))
                                temp = []

                if len(temp) != 0:
                    f.write(''.join(temp))
                    temp = []

                fs.close()

            else:
                fp = open("S" + str(i)+".txt")
                Lines = fp.readlines()

                if (len(Lines) > no_of_tuples_per_block * mm_size):
                    self.close_files(fpr, fps, f)
                    print("Size of sublist is more than main memory")
                    exit(0)

                for line in Lines:
                    l.append(line.split(" "))

                fp.close()

                fs = open("R" + str(i)+".txt")

                temp = []
                while True:
                    p = fs.readline()
                    if (len(p) == 0):
                        break
                    p = p.split(" ")
                    for j in range(len(l)):
                        if p[1][:-1] == l[j][0]:
                            s = str(p[0])+" "+str(l[j][0]) + \
                                " " + str(l[j][1])
                            temp.append(s)
                            if (len(temp) == self.no_of_tuples_per_block):
                                f.write(''.join(temp))
                                temp = []

                if len(temp) != 0:
                    f.write(''.join(temp))
                    temp = []

                fs.close()
        return f

    def close_files(self, fpr, fps, f=None):
        l = len(fpr)
        for i in range(0, l):
            os.remove(fpr[i].name)

        l = len(fps)
        for i in range(0, l):
            os.remove(fps[i].name)
        if f!=None:
            f.close()


if join_type == "sort":
    s = SortMergejoin(R_file, S_file, tuples, no_of_tuples_per_block)
    fpr, fps, f = s.sort_files(True)
    s.close_files(fpr, fps, f)
elif join_type == "hash":
    h = HashJoin(R_file, S_file, no_of_tuples_per_block)
    fpr = h.open_files("R")
    fps = h.open_files("S")
    f = h.join_files(fpr, fps)
    h.close_files(fpr, fps, f)
else:
    print("Join type not found")
    exit(0)

end_time = time.time()