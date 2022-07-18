%Plot of Region of Absolute Stability (script 2)-AB_2step
clc
clear
syms theta 
z=exp(1i*theta);
hlamda = (2*(z^2 - z))/(3*z-1); 
H=[];
THETA=[];
for i = 0:pi/30:2*pi
THETA = [THETA i];
H = [H subs(hlamda,theta,i)];
end
a = max(size(THETA));
fprintf('   \x03B8     Re(h\x03bb)     Im(h\x03bb)\n')
fprintf('------------------------------\n')
for i = 1:a
    fprintf('%4i  %8.2f %10.2f\n', ...
        round((THETA(i)/pi)*180),double(real(H(i))),double(imag(H(i))))
end
fill(real(H),imag(H),'c');
set(gca,'FontSize',12);
xlabel('Re (h\lambda)');
ylabel('Im (h\lambda)');
title('Region of Absolute Stability for 2-step AB method');
axis([-1 0.4 -1 1]);
grid on;



