%6G6Z3002 - Computational methods in ODEs
%Interval of Absolute Stability (script 2)
%Matlab script for producing a table for interval of absolute stability
%using Root Locus Method of a given LMS method- here AM2-step
%Note: the result is printed in rows as given in the lecture notes
%
out=[];
Hlam=[];
r1=[];
r2=[];
count=0;
h=2;
while h >= -7
count=count+1;
h=h-1;   
p=[(1-5*h/12) -(1+8*h/12) (h/12)];
r=roots(p);
Hlam(count)=h;
r1(count)=r(1);
r2(count)=r(2);   
end
disp(count-1);
for i=count-1:-1:1
fprintf('%5.2f\t',Hlam(i))
end
fprintf('\n')
fprintf('----------------------------------------------------------------------\n');
for i=count-1:-1:1
fprintf('%5.2f\t',abs(r1(i)))
end
fprintf('\n')
for i=count-1:-1:1
fprintf('%5.2f\t',abs(r2(i)))
end
fprintf('\n')

