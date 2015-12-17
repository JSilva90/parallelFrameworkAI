import multiprocessing as mp
import sys


##This is a parallel abstraction using forks to The Allen AI Science Challenge
##In case you have any question you can communicate with me using the following mail: up201007483@fc.up.pt

## receives a list and a number of processes
## returns a list of lists, each inner list contains the data associated to a process
def divideData(data, n_proc):
    partitioned_lists = []
    for i in range(0, n_proc):
        partitioned_lists.append([])
    
    l = 0
    for i in range(0, len(questions)):
        partitioned_lists[l].append(questions[i])
        l += 1
        l = l % n_proc
    return partitioned_lists

##This function represents the strategy to obtain the answer for a single question.
## If you have a global model for every question or any other global information you can add it as a parameter of this function
## and in the mp.Process part
def answerQuestions(questions, lock, com):
    for question in questions:
        print question.question ##In my class it prints the question, I advise to remove this since it'll run in parallel.
        ##apply your strategy to find the answer for a single question
    
    ##after answering all questions its time to sent the results back
    ##in my case I store the obtained answer into my own question class and just retrieved the classes back with the answer
    ##You can count the errors and correct answers within this function and just return those numbers. It's really up to you.
    
    ##sending results time
    lock.acquire() ##obtain lock to send
    out_pipe, in_pipe = com
    if out_pipe.poll(): ##if it has a message, it means some other process already left their results in the pipe
        ##so this one has to read the previous results, append them to its own results and put the whole thing in the pipe
        msg = out_pipe.recv()
        questions = questions + msg
    in_pipe.send(questions) ##the first process to send results don't need to read anythin just posts its results in the pipe
    lock.release() ## release lock
    
    ##after this every process other than the main will terminate. The main will continue with the execution of the main function
    
    
##main function
def main():
    if __name__ == "__main__": ##This is required to use forks correctly
        try:
            n_proc = int(sys.argv[1]) ##read number of processes from the line
        except Exception as e:
            print "Indicate the number of cores to use... ex: python aikaggle.py 4"
            quit()
        
        #use a function to read the data
        data = readData("filename.tsv")
        
        ##I'm suppossing your data is a list of anything. In my case I created my own class that stores
        ##the question itselt, its id, options, correct answer (in case it is available) and some other useful information for my approach
        
        ##next step is to divide the work among all processes. I'm not doing any work balance strategy, 
        ##I just equally divide the number of questions among processors and assume each one will work similarly the same amount of time
            
        partitioned_lists = divideData(data, n_proc) ##gets a list of lists, each inner list is the data assigned to a process
        ##Once again in my case I just divide the questions among processes
                
        ##define parallel variables
        lock = mp.Lock() ##used to avoid concurrent writes on pipe
        com = mp.Pipe() ## for interprocess communication
        workers = [] ## keep track of the workers
        
        ##The main process will start all the others
        for i in range(1, n_proc):
            ##workOnData is the function that actually do whatever your approach does, also this is the part that returns in parallel
            p = mp.Process(target=answerQuestions, args=(partitioned_lists[i], lock, com))
            workers.append(p)
            p.start()
        
        ##send the main worker to actually process some work too 
        answerQuestions(partitioned_lists[0], lock, com)
        
        for w in workers: ##main process waits for all the others to finish
            w.join()
            
        ##reads the final output from every process
        out_pipe, in_pipe = com
        final_results = out_pipe.recv()
        
        ##do whatever you want with the results
        ##in my case I iterate through all the questions and compare if my selected answer is the same as the correct one
        ##calculating the error rate of the approach
    


main()
