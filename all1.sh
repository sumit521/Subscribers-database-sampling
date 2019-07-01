# shell script for taking samples (0.1% or 0.01%) from subscriber databse records(multiple types of records) of multiple service providers simultaneously
# See CAF sampling guide for detailed operation. 
# This code only works if first column(or first 10 characters) of file are mobile numbers. Some lines may not have that those lines
# are ignored. But, in general first column should be 10 digit mobile number only. 
#!/bin/sh 
echo -e "$(tput setaf 4)                      $(tput setab 2)START OF SAMPLING $(tput sgr0)"
sed -i 's/\r$//g' paths.txt # to convert is to unix format
sed -i  '/^[[:alpha:]\/]/!d' paths.txt        #paths.txt should have directory addresses starting with an alphabet only
readarray paths < paths.txt 
x=3 # percentage of samples that you want to take extra 

# DEFINING FUNCTIONS

#point_one and point_zero_one correspond to 0.1% or 0.01% samples of total subscribers.
point_one(){ 
q=$(awk '/^[[:digit:]]/{a++}END{print a}' dbmerged.txt) # finding the cout of lines starting with a number.
if [[ -z "$q" ]];#checking for null strings
then
q=0
s=$(cat dbmerged.txt | wc -l)
if (($s!=0))  
then
 echo -e "$(tput setaf 7)$(tput setab 1)ERROR HAPPENED. First column is not mobile numbers.
Ask TSP to submit database files with first column as mobile numbers
and then run the code afresh.$(tput sgr0)\n"
exit
fi
fi
if ((($q%1000-$q%100)/100>=5))  # rounding the decimal place. eg 114.63 rounded off to 115
then
((t=$q/1000+1 ))
else
((t=$q/1000 ))
fi
}

point_zero_one(){
q=$(awk '/^[[:digit:]]/{a++}END{print a}' dbmerged.txt)
if [[ -z "$q" ]];
then
q=0
s=$(cat dbmerged.txt | wc -l)
if (($s!=0))  
then
 echo -e "$(tput setaf 7)$(tput setab 1)ERROR HAPPENED. First column is not mobile numbers.
Ask TSP to submit database files with first column as mobile numbers
and then run the code afresh.$(tput sgr0)\n"
exit
fi
fi
if ((($q%10000-$q%1000)/1000>=5))
then
((t=$q/10000 +1 ))
else
((t=$q/10000 ))
fi
}

# func1 will take  0.1% or 0.1% + some extra samples
func1(){
((b = $t+$x\*$t/100))
shuf -n $b dbmerged.txt > presam.txt # some extra than desired number of samples
echo total number of samples after shuffling =$b
sed -i  '/^[[:digit:]]/!d' presam.txt 	#to delete rows not starting with a digit
v=$(cat presam.txt | wc -l)
echo rows deleted after formatting for non digit rows = $(( $b-$v))
cut -c -10 presam.txt > numbers.txt   # cutting the first 10 characters to get mobile numbers. 
tail -q -n+1 mdn.txt >> numbers.txt # add mdn.txt number(numbers from history of previous months)s to bottom of numbers.txt
awk  'X[$1]++ == 1 {print $0}' numbers.txt > duplicates.txt #printing duplicate numbers in a file
c=$(cat duplicates.txt | wc -l)
echo duplicate numbers=$c. Deleting these numbers....
((c--)) #for array index purpose
readarray arr < duplicates.txt  # copying duplicate numbers to array
while(($c >= 0))
 do
  ((d=${arr[$c]}))   
  sed -i  "/^$d/d" presam.txt   # delete the line which start with duplicate numbers from the sampled file. 
 ((c--))
 done
e=$(cat presam.txt | wc -l)
((e=$e-$t))
echo deleting $e extra lines to get $t samples. 
if (($e<=0))
then
mv presam.txt samples.txt
else
sed "1,${e}d" presam.txt > samples.txt  # delete the remaining lines to get  $t samples.
fi
z=$(cat samples.txt | wc -l)
}

sampler(){
func1
while(($z < $t))
do
((x=$x+10))
echo -e "$(tput setaf 6)\nWe are in loop. So we take extra samples for shuffling. The value of x(extra samples) is $x%$(tput sgr0)"
func1
done
x=3
cut -c -10 samples.txt > p2.txt
j=$(head -n 1 dbmerged.txt)
if [[ -n "$j" ]];
then
if [[ ${j:0:1} != [[:digit:]] ]]; 
then
sed -i "1i$j" samples.txt
else 
j=$(echo first line of database file is not column names)
sed -i "1i$j" samples.txt
fi
fi
}

#mdn_updater() is used to add the numbers sampled to the previous list of history numbers stored in mdn.txt
mdn_updater(){
cd numbers
echo -e "$(tput setaf 3)         \nRemoving duplicates if any from new sample files for mdn updation $(tput sgr0)\n"
cat *.txt > p2.txt
awk 'X[$1]++ == 1 {print $0}' p2.txt > duplicates.txt
c=$(cat duplicates.txt | wc -l)
echo -e "\n$(tput setaf 5)duplicate numbers among various samples for six types of files =$c. Deleting these numbers...$(tput sgr0)\n\n"
readarray arr < duplicates.txt 
((c--)) # for array index purpose
while(($c >= 0))
 do
  ((d=${arr[$c]}))   
  sed -i  "/$d/d" p2.txt
 ((c--))
 done
tail -q -n+1 duplicates.txt >> p2.txt
cp ../mdn.txt mdn.txt
z=$(cat p2.txt | wc -l)
tail -q -n+1 mdn.txt >> p2.txt
}

mdn_constant(){
mdn_updater
head -n -$z p2.txt >p3.txt # delete as many numbers added from bottom as added to top so that total coutn remains constant
mv p3.txt mdn_updated.txt
mv mdn_updated.txt ../results
rm mdn.txt p2.txt duplicates.txt
cd ..
}

mdn_incremental(){
mdn_updater
mv p2.txt mdn_updated.txt #here only new samples are added at top but not deleted from bottom.
mv mdn_updated.txt ../results
rm mdn.txt  duplicates.txt
cd ..
}

#In columns_selector we are taking only four solumns(1,2,3 and4) from the sample file. this can be changed to your requirement
columns_selector(){
cd results
mkdir columns_reduced
a=1;b=2;c=3;d=4  
awk -F'[|,;\t]' '{print $'$a',$'$b',$'$c',$'$d'}' OFS='|' samples_pre_ekyc.txt  >reduced_pre_ekyc.txt #any one of thes[|,;\t] will be take as delimiter. You can delete 3 of them if you are sure of the delimiter used
mv reduced_pre_ekyc.txt columns_reduced
awk -F'[|,;\t]' '{print $'$a',$'$b',$'$c',$'$d'}' OFS='|' samples_post_ekyc.txt >reduced_post_ekyc.txt
mv reduced_post_ekyc.txt columns_reduced
awk -F'[|,;\t]' '{print $'$a',$'$b',$'$c',$'$d'}' OFS='|' samples_bulk_ekyc.txt >reduced_bulk_ekyc.txt
mv reduced_bulk_ekyc.txt columns_reduced
awk -F'[|,;\t]' '{print $'$a',$'$b',$'$c',$'$d'}' OFS='|' samples_pre_paper.txt >reduced_pre_paper.txt
mv reduced_pre_paper.txt columns_reduced
awk -F'[|,;\t]' '{print $'$a',$'$b',$'$c',$'$d'}' OFS='|' samples_post_paper.txt >reduced_post_paper.txt
mv reduced_post_paper.txt columns_reduced
awk -F'[|,;\t]' '{print $'$a',$'$b',$'$c',$'$d'}' OFS='|' samples_bulk_paper.txt >reduced_bulk_paper.txt
mv reduced_bulk_paper.txt columns_reduced
cd ..
}
#logic for each directory mentioned in paths.txt starts here
for p in "${paths[@]}"
do
cd $p && pwd
mkdir results
mkdir numbers

if [[ "$(head -1 mdn.txt)" == *$'\r' ]] ## to check if mdn.txt is in DOS format or unix. 
#Generally for the very first time mdn.txt will be in DOS format and will have some errors. To clean it up(remove duplicates and random characters) and convert to unix this condition is tested.
 then
 echo -e "$(tput setaf 5)          Formating and removing duplicates from mdn.txt(history file) $(tput sgr0)"
sed -i -n -e 's/.*\([0-9]\{10\}\).*/\1/p' mdn.txt # to delete anything other than last 10 consecutive non zero digits mobile number
## to delete duplicate values in mdn.txt if any
awk  'X[$1]++ == 1 {print $0}' mdn.txt > duplicates.txt
c=$(cat duplicates.txt | wc -l)
echo -e "\n$(tput setaf 3)duplicate numbers in mdn.txt =$c . Deleting these numbers...$(tput sgr0)"
readarray arr < duplicates.txt 
((c--)) # for array index purpose
while(($c >= 0))
 do
  ((d=${arr[$c]}))   # this line shows error in prompt but only for first time but our required output is not affecteted
  sed -i  "/$d/d" mdn.txt
 ((c--))
 done
 tail -q -n+1 duplicates.txt >> mdn.txt #add duplicate value because above loop removes both the number and its duplicate
 rm duplicates.txt
fi 

 ###
 
cd pre_ekyc
cat *.txt >dbmerged.txt # merging all the .txt files in pre_ekyc folder
cd ..
cd pre_paper
cat *.txt >dbmerged.txt
cd ..
cd post_ekyc
cat *.txt >dbmerged.txt
cd ..
cd post_paper
cat *.txt >dbmerged.txt
cd ..
cd bulk_ekyc
cat *.txt >dbmerged.txt
cd ..
cd bulk_paper
cat *.txt >dbmerged.txt
cd ..
echo pre_ekyc pre_paper post_ekyc post_paper bulk_ekyc bulk_paper  | xargs -n 1 cp mdn.txt # copying history file (mdn.txt) to all 6 folders


###
cd pre_ekyc # enter into prepaid ekyc subscribers folder
point_one #find 0.1% sample count
echo -e "\n\n$(tput setaf 4) $(tput setb 7)total number of prepaid ekyc subscribers = $q. 0.1% sample count should be  $t$(tput sgr0)\n"
sampler # take 0.1% sample 
mv p2.txt p2_pre_ekyc.txt
mv p2_pre_ekyc.txt ../numbers # move sampled numbers to numbers folder
mv samples.txt samples_pre_ekyc.txt
mv samples_pre_ekyc.txt ../results # move sapled file to results folder
find . ! -name 'dbmerged.txt' -type f -exec rm -f {} + #remove all files other than dbmerged.txt from the pre_ekyc folder
mv dbmerged.txt database.txt # rename it to database.txt (if you re run the program without this line then the cat *.txt>debmerged.txt command will show error.)
cd ..

###
cd pre_paper
point_one
echo -e "\n\n$(tput setaf 4) $(tput setb 7)total number of prepaid paper subscribers = $q. 0.1% sample count should be  $t$(tput sgr0)\n"
sampler
mv p2.txt p2_pre_paper.txt
mv p2_pre_paper.txt ../numbers
mv samples.txt samples_pre_paper.txt
mv samples_pre_paper.txt ../results
find . ! -name 'dbmerged.txt' -type f -exec rm -f {} +
mv dbmerged.txt database.txt
cd ..

###
cd post_ekyc
point_one
echo -e "\n\n$(tput setaf 4) $(tput setb 7) total number of postpaid ekyc subscribers = $q. 0.1% sample count should be  $t$(tput sgr0)\n"
sampler
mv p2.txt p2_post_ekyc.txt
mv p2_post_ekyc.txt ../numbers
mv samples.txt samples_post_ekyc.txt
mv samples_post_ekyc.txt ../results
find . ! -name 'dbmerged.txt' -type f -exec rm -f {} +
mv dbmerged.txt database.txt
cd ..

###
cd post_paper
point_one
echo -e "\n\n$(tput setaf 4) $(tput setb 7) total number of postpaid paper subscribers = $q. 0.1% sample count should be  $t$(tput sgr0)\n"
sampler
mv p2.txt p2_post_paper.txt
mv p2_post_paper.txt ../numbers
mv samples.txt samples_post_paper.txt
mv samples_post_paper.txt ../results
find . ! -name 'dbmerged.txt' -type f -exec rm -f {} +
mv dbmerged.txt database.txt
cd ..

###
cd bulk_ekyc
point_zero_one
echo -e "\n\n$(tput setaf 4) $(tput setb 7) total number of bulk ekyc subscribers = $q. 0.01% sample count should be  $t$(tput sgr0)\n" 
sampler
mv p2.txt p2_bulk_ekyc.txt
mv p2_bulk_ekyc.txt ../numbers
mv samples.txt samples_bulk_ekyc.txt
mv samples_bulk_ekyc.txt ../results
find . ! -name 'dbmerged.txt' -type f -exec rm -f {} +
mv dbmerged.txt database.txt
cd ..

###
cd bulk_paper
point_zero_one
echo -e "\n\n$(tput setaf 4) $(tput setb 7) total number of bulk paper subscribers = $q. 0.01% sample count should be  $t$(tput sgr0)\n"
sampler
mv p2.txt p2_bulk_paper.txt
mv p2_bulk_paper.txt ../numbers
mv samples.txt samples_bulk_paper.txt
mv samples_bulk_paper.txt ../results
find . ! -name 'dbmerged.txt' -type f -exec rm -f {} +
mv dbmerged.txt database.txt
cd ..

###
columns_selector # set the columns tou want in columns_selector function. Don't forget to save this file after making any changes.
mdn_constant # history file count will remain same. To have option of not deleting previous numbers, put '#' at start of this line and remove '#' from next line (#mdn_incremental)
#mdn_incremental
echo -e "$(tput setaf 2)TSP sampling completed at $p $(tput sgr0)\n"
done
cd 

echo -e "$(tput setaf 7)                       $(tput setab 1)END OF SAMPLING $(tput sgr0)"
###end of code
# mapping : n1.txt -duplicates.txt , p1.txt-numbers.txt 

