from nltk.corpus import wordnet
import sys
from pyspark import *
import math
conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf)

 # creat an RDD from of text file
textFile = sc.textFile("/Users/samson/Desktop/Encrypted-1.txt")

#map the RDD in a line and then flatMap each line of RDD to split each word and creat  RDD for each word
words = textFile.map(lambda line : str(line)).flatMap(lambda line : line.split())
#map the RDD in a line and then flatMap each line of RDD to split each character and creat RDD for each character
chars = textFile.map(lambda line :str(line)).flatMap(lambda line:list(line))

#Total number of words and chars
total_Word=words.count()
total_Char=chars.count()
#print the total chars and words
print("the total word is",str(total_Word))
print("the total char is",str(total_Char))

#map value of one to each characters by using .map()
#get average occurance of each chararacters by dividing it to the total chrs of the document
#filter the rdd to be only letters
#sort the rdd in decending order based on it's occurance
char_count=chars.map(lambda char: (char, 1)).reduceByKey(lambda x,y: x+y).map(lambda char:(char[0],char[1],(char[1]/float(total_Char)*100))).filter(lambda char: char[0].isalpha()).sortBy(ascending=False, numPartitions=None, keyfunc = lambda char:char[2])

#get the first element in the rdd as tuple
letter=char_count.first()
# get the letter from the tuple
letter=str(letter[0])
#print the frequently occur letter in the file
print(letter," is the frequently occur letter")
# letters in English alphabet
stri='ABCDEFGHIJKLMNOPQRSTUVWXYZ'
# letters in English alphabet based on their occurance
letters=['E','T','A','O','I','N','S','H','R','D','L','C','U','M','W','F','G','Y','P','B','V','K','J','X','Q','Z']
#initialize an iterator variable i; that will help to iterate thru the letters array
i=0

# funaction that takes the most occur lettter in the file and the iteration index i as parameter and return the difference between the index of the letter  and the index of the most occur letter in English
def compare(letter,i):
    #find the difference and save it as value
    value=stri.index(letter)-stri.index(letters[i])
    #to make sure onlt positive value is returned
    if value<0:
        value= -1*value
    #return the value
    return value

#function to shift words in the file by the value
def shift():
    value=compare(letter,i)
    #creat an empty array data[]
    data=[]
    # iterate thru the characters of the text
    for ch in chars.collect():
        #condtion if the character is alphabet and it is in stri list
        if ch.isalpha() and ch in stri:
            # add the value to the index of the letter and check if it is not above 26; if it is above 26, it will use the remender as the new index for the new words
            #then it will append the new word to the data array
            data.append(stri[(stri.index(ch) + value) % 26])
            #if the character is not a letter, it will just attach it to the arraya
        else:
            data.append(ch)
            #join the array to a single string file
            output =''.join(data)
    #return the new file
    return output
newfile=shift()

#function that give sampes from newly created text file
def gsamp():
    # it will split the new file in to array by making empty space as a speration factor
    new_text=shift().split(" ")
    # creat an RDD from the array of new file with partion of 10
    ntextFile=sc.parallelize(new_text,numSlices=10)
    #filter the rdd to make it only words
    new_words = ntextFile.filter(lambda word: word.isalpha())
    #creat an empty array, arryOfSamples
    arryOfSamples = []
    #take the first 10 element of the rdd
    arryOfSamples=new_words.take(10)
    #return array sample of words from the newly created text file
    return arryOfSamples

#fuction that takes samples then return the result if they are English words or not
def tester():
    arryOfSamples  = gsamp()
    #tally to check the amount of english in the sample
    tally=0
    #iterate thru the sample
    for samp in arryOfSamples:
        #if an English word found add one to the tally
        if wordnet.synsets(samp):
            tally=tally+1
            #if an English word is not found, continue the loop
        else:
            continue
    return tally
#function that takes the tally then make descition
def checker():
    tally=tester()
        #if tally is greater than 5, the sample of words are pass the test which means they are English words
    if tally>5:
        #ask the user to enter File name to save as
        inputt=input("Enter the file name to descript: ")
        #save the new file file as format given by the user
        with open("{}.txt".format(inputt),'w+') as f:
            f.writelines(newfile)
            f.close()
    else:
        #if the tally is less than 5, which means it fails the test so it have to start to compare the letter from the orgianl file and the next letter inletters to get a new difference.
        # declear i as global variable because it already decleared in another function
        global i
        #incriment i by adding 1 to it
        i=i+1
        #recursivly return new value of i to compare fuction
        #recursiv functions that going to excute untill the text file passes the test
        compare(letter,i)
        shift()
        gsamp()
        tester()
        checker()
checker()
