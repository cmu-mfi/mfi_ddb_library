from blabel import LabelWriter
import json

file_id = '78d989d5edda'
label_size = "S" # XS - ID only | S - 2X2 in | M - 4X4 in | L - 4X6 in

if label_size == "S":
    label_writer = LabelWriter("label_template_S.html",default_stylesheets=("styleS.css",))
elif label_size == "L":
    label_writer = LabelWriter("label_template_L.html",default_stylesheets=("styleL.css",))

with open(f'{file_id}.json') as json_file:
    data = json.load(json_file)

label_writer.write_labels([data], target=f'{file_id}_label.pdf')