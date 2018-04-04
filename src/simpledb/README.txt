Developed by Andrew Petit and Daniel Yun.

Installation Instructions / starting the database server:

1:
    First ensure that Java 1.8 is installed and Intellij is configured correctly
    on your local machine. Once it is, you may need to increase the memory used by
    Intellij to have the program work correctly. To do this, navigate to the help menu
    item at the top of the screen and select edit custom vm options. From here set the
    row starting with -Xmx to have 2048m of space.

2:
    To get our project, download the zip file with the source code in it from canvas.
    Then simply unzip the file to your choice of locations and then in Intellij open the
    project.

3:
    To run the code, navigate to the startup file in the src/server directory of the project.
    Edit the run configurations to pass both a directory name (can be anything, but for simplicity
    it might make sense to call this directory CS4432DB) and a replacement policy (should be either
    LRU or CLOCK) as run time arguments. Then you can run the program and the database server should
    start right up.

Running database transactions:

1:
    You will need to either create a new JDBC file modeled after Examples.java with specific
    queries, inserts, updates, and other database transactions, or simply run the file Examples.Java
    which will execute a number of transactions. If you did create a new file, assuming you set it up correctly,
    you should be able to just run it and go.