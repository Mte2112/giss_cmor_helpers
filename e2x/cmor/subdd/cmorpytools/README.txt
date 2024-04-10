SCRIPTS AND DOCUMENTATION

~auto_cmorall_subdd.py~
Description: An all-in-one subdaily CMORization wrapper for GISS ModelE E2.X output
Features: CMORization, diagnostic corrections, file compression via one-liner
Key Scripts: 
-subdd/cmorall.py includes CMOR and compression steps
-subdd/corrections.py includes code relevant for corrections of known diagnostic issues
Usage: python auto_cmorall_subdd.py runname startyear endyear compress
Notes:
-Optional 5th argument "compress" to turn on compression
-If running with nohup, try python -u auto_cmorall_subdd.py runname startyear endyear compress
-The optional -u argument ensures that the print statements are not buffered in *.out
-auto_cmorall_subdd.py should be executed from your CMOR directory (probably CMOR3.3.2)
-This directory (cmorpytools) should be copied to CMOR3.3.2 as well since the packages are imported via relative path
