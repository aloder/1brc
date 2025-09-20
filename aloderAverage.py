# from queue import Queue
from multiprocessing import Queue, Process, shared_memory
import cProfile
import os
import sys
import mmap


cpu_count = 10


def distribute_work_zero_copy(file_name: str, buffer_queue: list[Queue]):
    # Clean up old profile files
    import glob
    for old_profile in glob.glob("scan_profile*.prof"):
        try:
            os.remove(old_profile)
        except:
            pass

    prof = cProfile.Profile()
    prof.enable()

    with open(file_name, "rb") as f:
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            file_size = len(mm)
            num_workers = len(buffer_queue)
            chunk_size = file_size // num_workers

            for i in range(num_workers):
                start = i * chunk_size
                if i == num_workers - 1:
                    end = file_size
                else:
                    end = (i + 1) * chunk_size
                    # Adjust to end at newline
                    while end < file_size and mm[end:end+1] != b'\n':
                        end += 1
                    if end < file_size:
                        end += 1  # Include the newline

                # Send only metadata: filename and byte offsets
                buffer_queue[i].put((file_name, start, end))
                buffer_queue[i].put(None)

    prof.disable()
    prof.dump_stats("scan_profile.prof")

def main():
    lst_queue = [Queue(2) for _ in range(cpu_count or 10)]

    read = Process(target=distribute_work_zero_copy, args=(sys.argv[1], lst_queue,))
    workers = []
    return_queue = Queue()
    cord = Process(target=cordinator, args=(return_queue,))
    for q in lst_queue:
        actuate = Process(target=process_mmap_chunk, args=(q,return_queue,))
        workers.append(actuate)
    cord.start()
    for w in workers:
        w.start()
    read.start()
    read.join()

    for w in workers:
        w.join()

    return_queue.put(None)

    cord.join()




def cordinator(return_queue: Queue):
    results = {}
    while True:
        g = return_queue.get()
        if g == None:
            break
        for name, value in g.items():
            if not name in results:
                results[name] = value
            else:
                results[name][0] = min(results[name][0], value[0])
                results[name][1] = max(results[name][1], value[1])
                results[name][2] += value[2]
                results[name][3] += value[3]

    print("{", end="")
    for location, measurements in sorted(results.items()):
        print(f"{location.decode('utf8')}={measurements[0]:.1f}/{(measurements[2]/measurements[3]):.1f}/{measurements[1]:.1f}",
              end=", ")
    print("\b\b} ")



def process_mmap_chunk(buffer_queue: Queue, return_queue: Queue):
    results = {}
    prof = cProfile.Profile()
    prof.enable()

    while True:
        task = buffer_queue.get()
        if task == None:
            break

        file_name, start_offset, end_offset = task

        # Each worker creates its own mmap view - zero copy!
        with open(file_name, "rb") as f:
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                # Process only our assigned chunk
                pos = start_offset

                while pos < end_offset:
                    # Manual scan for newline - replace mmap.find()
                    nl_pos = pos
                    while nl_pos < end_offset and mm[nl_pos] != 10:  # 10 = ord('\n')
                        nl_pos += 1

                    if nl_pos >= end_offset:
                        break

                    line_start = pos
                    line_end = nl_pos

                    # Manual scan for semicolon - replace mmap.find()
                    sc_pos = line_start
                    while sc_pos < line_end and mm[sc_pos] != 59:  # 59 = ord(';')
                        sc_pos += 1

                    if sc_pos < line_end:
                        name = mm[line_start:sc_pos]
                        val = float(mm[sc_pos + 1:line_end])

                        if name not in results:
                            results[name] = [val, val, val, 1]  # min, max, sum, count
                        else:
                            current = results[name]
                            if val < current[0]:
                                current[0] = val
                            elif val > current[1]:
                                current[1] = val
                            current[2] += val
                            current[3] += 1

                    pos = nl_pos + 1

    return_queue.put(results)
    prof.disable()
    prof.dump_stats(f"scan_profile-{os.getpid()}.prof")






if __name__ == "__main__":
    main()
