# IBMIOSS Demo Data
Create demo data for IBMIOSS demo applications

### Requirements
Python 3 with the following packages
* ibm_db
* itoolkit


### Objects Created
* IBMIOSSRCV - journal receiver
* IBMIOSSJRN - journal
* CONTACT - 100,000 random names/addresses (addresses are not real addresses)

### Usage
    python3 build_ibmioss_demo_data <library-name>

&lt;library-name&gt; is optional - if not provided the pgm will create a library called IBMIOSS to hold the objects.

Journaling is added to the CONTACT file dumping to the IBMIOSSRCV journal receiver.
