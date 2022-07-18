%6G6Z3002 - Computational methods in ODEs 
%Plot of Region of Absolute Stability (script 1) – AB 2step
clear
syms theta 
hlamda = (2*(exp(2i*theta) - exp(1i*theta)))/(3* exp(1i*theta)-1);
H=[];
THETA=[];
for i = 0:pi/30:2*pi
THETA = [THETA i];
H = [H subs(hlamda,theta,i)];
end
a = max(size(THETA));
fprintf('  theta      real(hlamda)     imag(hlamda)\n')
fprintf('-----------------------------------------------\n')
for i = 1:a
   fprintf('%6i %8.2f %12.2f\n',...
       round((THETA(i)/pi)*180),double(real(H(i))),double(imag(H(i))))
end
plot(real(H),imag(H),'LineWidth',2);
set(gca,'FontSize',12);
xlabel('Re (h\lambda)');
ylabel('Im (h\lambda)');
title('Region of Absolute Stability for 2-step AB method');
axis([-1 0.4 -1 1]);
grid on;
