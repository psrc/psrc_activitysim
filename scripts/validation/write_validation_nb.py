import os, sys, shutil
import nbformat
from nbconvert.preprocessors import ExecutePreprocessor

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
output_path = "outputs/validation"

# list of all validation notebooks
all_validation_nb = ['auto_ownership/auto_ownership',
                     'cdap/cdap',
                     'school_location/school_location',
                     'school_location/school_location_unweighted',
                     'work_location/work_location',
                     'work_location/work_location_unweighted']
# all_validation_nb = ['auto_ownership/auto_ownership']


def run_ipynb(sheet_name, nb_path):
    with open(nb_path + r'/' + sheet_name + ".ipynb") as f:
        nb = nbformat.read(f, as_version=4)
        if sys.version_info > (3, 0):
            py_version = 'python3'
        else:
            py_version = 'python2'
        ep = ExecutePreprocessor(timeout=1500, kernel_name=py_version)
        ep.preprocess(nb, {'metadata': {'path': nb_path + r'/'}})
        with open(nb_path+r'/'+sheet_name+".ipynb", 'wt') as f:
            nbformat.write(nb, f)
    print(sheet_name + " validation notebook created")


def main():
    for sheet_name in all_validation_nb:
        run_ipynb(sheet_name, CURRENT_DIR)

    # render quarto book
    text = "quarto render"
    os.system(text)
    print("validation notebook created")

    # Move these files to output folder
    # if os.path.exists(os.path.join(os.getcwd(),output_path,"validation-notebook")):
    #     os.remove(os.path.join(os.getcwd(),output_path,"validation-notebook"))
    # os.rename((os.path.join(CURRENT_DIR,"validation-notebook"))), os.path.join(os.getcwd(),output_path,"validation-notebook")))


if __name__ == '__main__':
    main()
