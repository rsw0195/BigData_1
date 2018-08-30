Part 1 and Part 2:  Use DataA1.csv as the input.  Data overview:  The data is in a CSV file.  The rows are Orders.  The first column is Customer ID Number, the second column is Item ID Number, and the third column is Amount Spent on the Order.  Note, that Customer ID Number and Item ID Number repeat (i.e., a customer will have multiple orders, and an item will be ordered more than once).

Part 1:
Use Python to create a MapReduce program to determine the Total Amount spent by Customer.  Execute.  Submit your code (.py file) and output (.txt file).  Put your execution statement as the final line in your code (# commented out).

Hints:
Converting a string (stringtonumber) to a number (floating point):  float(stringtonumber)

Adding up numbers coming into a reducer (value):  sum(value)

Part 2:
Use Python to create a MapReduce program to sort the Total Amount spent by Customer (from the Customer who spent the least to the Customer who spent the most).  Execute.  Submit your code (.py file) and output (.txt file).  Put your execution statement as the final line in your code (# commented out).

Hints:
You will probably need to chain two MapReduce jobs together.

Code Snippet:  '%04.02f'%float(order)
Allows you to change a variable that is a float(order) to give it leading zeros.  Where “order” was a float.

Part 3:
Find your own data set.  Come up with a “business problem” to address using MapReduce, and complete it.  Provide (~half page of text) an Executive Summary (that describes the data set, the business problem, what you did to solve it, and the overall answer); you may include a figure – but don’t exceed 1 page total.  Include your code, output, and a link to the data set (if the data set is less than 2MB, then feel free to include it directly).
