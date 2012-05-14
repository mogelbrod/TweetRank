#!/bin/bash
LANG=C;
awk -F= 'BEGIN{s=0.0;m=1E10;M=0.0}
{
 s += $2; 
 if ($2 < m) m = $2;
 if ($2 > M) M = $2;
}
END{print "s =",s,"m =",m,"M =",M; }' $1
