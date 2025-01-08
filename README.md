# arrow-dataset-manipulation
This repo contains old and refactored versions of code for manipulation of Arrow datasets.

Notes on the refactored code:
1. No for loops are being used to process the data anymore which noticeably improves the runtime 
2. Replaced hard-coded variables in the code's logic with variables.
3. There was no obvious pattern in how the data was being manipulated in the provided code, which is why the variable names are not very expressive at times in the refactored code. Usually that would depend on the application at hand.


The following graph displays the runtime of the old code vs. the new code with varying the input file size.


![image](https://github.com/user-attachments/assets/c6a811e3-f7b9-4933-b4c8-d141fa772838)
