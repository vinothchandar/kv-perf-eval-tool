# separate out the warmgc log from actual run 
# usage ./separateWarmup.sh <gclog file> <rampup gclog> <output gclog>
rampuplines=`wc -l $2 | python -c "import sys; [sys.stdout.write(line.split(' ')[0]) for line in sys.stdin]"`;
totallines=`wc -l $1 | python -c "import sys; [sys.stdout.write(line.split(' ')[0]) for line in sys.stdin]"`;
runstart=`expr $totallines - $rampuplines`;
tail -$runstart  $1 > $3

