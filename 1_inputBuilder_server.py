import os


#### CONSTANT VARIABLE DECLARATIONS
ROOT_PATH='/media/DATA2/burak/emission_job'
FASTA_DATA_PATH=os.path.join(ROOT_PATH,'uniclust50_2018')
FASTA_INPUT_LIST_FILE='fake_fasta_list.input' #holds the splitted fasta file names
FASTA_INPUT_JOB_FOLDERS=['inputs/log','inputs/txt','inputs/atom','inputs/csv','inputs/filtered_csv']
FASTA_OUTPUT_JOB_FOLDERS=['outputs/data','outputs/image','outputs/log']

#WAVELENGTH_DB_ROOT=os.path.join('/media/burak/yedek/emission_job')
WAVELENGTH_DB_DATA_PATH=os.path.join(ROOT_PATH,'db')
SPECTRUM_IMAGES_PATH=os.path.join(ROOT_PATH,'png_files')

MAX_IMG_SIZE=4000
JPEG_QUALITY=80
### INIT FOLDERS AND PATHS PART
def helperInitFoldersAndPathsForGivenJob(job_dir=None,job_folders=None):
    folder_holder=[]
    for job_folder in job_folders:
        folder_list=job_folder.split('/')
        folder_holder.append(folder_list)
    print(folder_holder)
    # call the creator
    output_dirs=helperCreateDirectoryIfNotExists(root_dir=job_dir,folder_list=folder_holder)
    return output_dirs

def helperCreateDirectoryIfNotExists(root_dir=None,folder_list=None):
    # job 0. create nested folders with given (folder_list> data if not exists
    output_dirs=[]
    recursive_dir=root_dir
    for folder_item in folder_list:
        for folder_name in folder_item:
            if not os.path.isdir(os.path.join(recursive_dir,folder_name)):
                os.mkdir(os.path.join(recursive_dir,folder_name))
            recursive_dir=os.path.join(recursive_dir,folder_name)
        output_dirs.append(recursive_dir)
        recursive_dir=root_dir
    print(output_dirs)


    return output_dirs

def helperFindElementFromGivenListByEndsWithKey(search_list=None,ends_with_key=None):
    found_val=None
    ends_with_key='/'+ends_with_key
    for item in search_list:
        if str(item).endswith(ends_with_key):
            found_val=item
            break
    print(found_val)

    return found_val

def helperReadFileInToList(file_path=None,file_name=None):
    with open(os.path.join(file_path,file_name)) as file:
        lines = [line.rstrip() for line in file]
    line_counter=len(lines)
    return lines,line_counter

### workerSplitFastaFileForRdkitInput Related Helper Step Functions
def workerFindStringBetweenTwoChars(start_char=None,end_char=None,search_string=None):
    # result_str=search_string[search_string.find(start_char) + len(end_char):search_string.rfind(end_char)] # gives error for some files
    # print(result_str)
    result_str=search_string.split(start_char)[1].split(end_char)[0] # this is more general solution
    #print(result_str)
    return result_str

def helperFindLastLineNumForGivenFile(file_path=None,file_name=None):
    line_counter=0
    with open(os.path.join(file_path,file_name)) as file:
        for index,raw_line in enumerate(file):
            line_counter+=1
    #print(line_counter)
    return line_counter

def helperFindLineNumsInFileStartsWith(file_path=None,file_name=None,search_str=None):
    found_indexes=[]
    with open(os.path.join(file_path,file_name)) as file:
        for index,raw_line in enumerate(file):
            if raw_line.startswith(search_str):
                found_indexes.append(index)
    #print(len(found_indexes))

    return found_indexes

def helperDetectSplitRangesForGivenFile(file_path=None,file_name=None,search_str=None):
    begin_vals,end_vals=[],[]
    # job 1. find line numbers starts with <search_str>
    range_vals=helperFindLineNumsInFileStartsWith(file_path=file_path, file_name=file_name, search_str=search_str)
    # job 2. get total line count (or index of last line) from given file
    last_line_index=helperFindLastLineNumForGivenFile(file_path=file_path, file_name=file_name)
    # job 3. arrange the ranges for <begin_vals> and <end_vals>
    begin_vals=range_vals # thats easy
    end_vals=range_vals[1:]#.append(last_line_index) # thats easy to just shift values
    end_vals.append(last_line_index)
    # job 4. convert list type ranges to range type
    read_ranges=list(zip(begin_vals,end_vals))
    return read_ranges

# begin of log operations related
def helperWriteFinishedProcessStepToLog(log_path=None,log_file=None,log_contents=None):
    # job 0. convert given write content to list if not
    if type(log_contents) is not list:
        log_contents=[log_contents]
    # job 1. write <log_contents> in to <log_file> to file according to given argumensts (process_name or file_type)
    with open(os.path.join(log_path,log_file), 'a+') as output_file:
        for write_content in log_contents:
            output_file.write(write_content + '\n')
    return None

def workerCheckGivenLineContainsString(input_line=None,contains_str=None):
    check_res=True if contains_str in input_line else False # https://stackoverflow.com/questions/2802726/putting-a-simple-if-then-else-statement-on-one-line
    return check_res

def looperCheckGivenLineContainsString(log_path=None,log_file=None,process_name=None):
    ### <process_name> argument is overridden to include <file_type> based log operations
    with open(os.path.join(log_path,log_file)) as log_file:
        for line in log_file.readlines():
            check_res=workerCheckGivenLineContainsString(input_line=line,contains_str=process_name)
            if check_res:
                break
    return check_res

def helperCheckLogFileAndGivenProcessLogRecordExistance(log_path=None,log_file=None,process_name=None):
    ### <process_name> argument is overridden to include <file_type> based log operations
    process_is_done=os.path.isfile(os.path.join(log_path,log_file))
    if process_is_done:
        # now check given <process_name> exists on <log_file>
        process_is_done=looperCheckGivenLineContainsString(log_path=log_path,log_file=log_file,process_name=process_name)
    return process_is_done

# end of log operations related

def workerReadAndConvertGivenFastaLinesToTextFile(file_path=None,file_name=None,read_range=None,save_dir=None):
    # job 1. init variables
    start,end=read_range
    # job 2. read given lines part
    with open(os.path.join(file_path,file_name)) as fasta_file:
        raw_data = fasta_file.readlines()[start:end]

    # job 3. convert readen <raw_data> to text part
    fasta_str=''
    for line in raw_data:
        if line.startswith('>'):
            # pull file name from special line
            save_file_name='%s.txt'%(workerFindStringBetweenTwoChars(start_char='|',end_char='|',search_string=line))
            print(save_file_name)
        else:
            # pull fasta content from regular lines
            fasta_str=fasta_str+line.strip()
    # job 4. write the pulled <fasta_str> contents in to disk with name <save_file_name> by checking file existence
    if not os.path.isfile(os.path.join(save_dir,save_file_name)):
        with open(os.path.join(save_dir,save_file_name), 'w') as output_file:
            output_file.write(fasta_str)
        # job 5. give info for loop call
        print('%s file saved succesfully on disk...'%(save_file_name))
    else:
        print('%s file already exists so skipping it...'%(save_file_name))                  
    return None


def workerSplitFastaFileForRdkitInput(file_path=None,file_name=None,save_dir=None,log_dir=None,search_str='>'):
    # job 0. detect fasta record ranges in given fasta file
    read_ranges = helperDetectSplitRangesForGivenFile(file_path=file_path, file_name=file_name, search_str=search_str)
    # job 1. conversation step
    for read_range in read_ranges:
        workerReadAndConvertGivenFastaLinesToTextFile(file_path=file_path,file_name=file_name,read_range=read_range,save_dir=save_dir)
    # job 2. write log record for given process
    log_file_name=save_dir.split('/')[-2]
    helperWriteFinishedProcessStepToLog(log_path=log_dir,log_file=log_file_name,log_contents=file_name)
    return None

def looperSplitFastaFileForRdkitInput(fasta_path=None,fasta_files=None,output_root_dir=None,log_root_dir=None):
    processed_dirs=[]
    # job 0. loop over files and call the process functions
    for fasta_file in fasta_files:
        # job 1. create given <fasta_file> related sub directory
        sub_folder_name = '%s_%s'%(fasta_file.split('.')[0],fasta_file.split('.')[1])
        if not os.path.isdir(os.path.join(output_root_dir, sub_folder_name)):
            os.mkdir(os.path.join(output_root_dir, sub_folder_name))
        output_process_dir=os.path.join(output_root_dir,sub_folder_name)
        print(output_process_dir)
        processed_dirs.append(output_process_dir)
        # job 2. Converting given fasta file items into txt format part
        log_file_name=output_root_dir.split('/')[-1]
        # print(log_file_name)
        # print(log_root_dir)
        is_process_step_done=helperCheckLogFileAndGivenProcessLogRecordExistance(log_path=log_root_dir,log_file=log_file_name,process_name=fasta_file)
        if not is_process_step_done:
            workerSplitFastaFileForRdkitInput(file_path=fasta_path,file_name=fasta_file,save_dir=output_process_dir,log_dir=log_root_dir)
        else:
            print('%s file is processed before so nothing newly done for this step...'%(fasta_file))
        #break

    return processed_dirs

def pipeHelperSplitFastaFileForRdkitInput(file_path=None,file_name=None,process_type='txt'): # <search_str> is constant for fasta files
    # job 0. read file into list part
    input_files,input_files_count=helperReadFileInToList(file_path=file_path,file_name=file_name)
    print(input_files)
    # job 1. initialize the job folders and paths
    process_dirs=helperInitFoldersAndPathsForGivenJob(job_dir=file_path,job_folders=FASTA_INPUT_JOB_FOLDERS)
    # job 2. detect function related output folder from given <process_dirs> argument by using <process_type> argument
    output_root_dir=helperFindElementFromGivenListByEndsWithKey(search_list=process_dirs, ends_with_key=process_type)
    log_root_dir = helperFindElementFromGivenListByEndsWithKey(search_list=process_dirs, ends_with_key='log')
    print(log_root_dir)
    processed_dirs=looperSplitFastaFileForRdkitInput(fasta_path=file_path,fasta_files=input_files,output_root_dir=output_root_dir,log_root_dir=log_root_dir)
    return processed_dirs

### CONVERTING FASTA STRINGS (TXT FILE) TO ATOMIC NUMBERS (ATOM FILE) PART
def workerWriteListContentsToFileSingleLine(list_content=None,save_dir=None):
    # job 0. write given <list_content> to file as single line
    with open(save_dir, 'w') as f:
        f.write(','.join(list_content))
    return None

def workerConvertGivenRdKitInputToAtomicNums(input_path=None,input_file=None,file_ext=None,output_dir=None):
    from rdkit import Chem
    # job 0. run if file not exists
    save_file_name = input_file.split('.')[0] + '.' + file_ext
    save_dir = os.path.join(output_dir, save_file_name)
    if not os.path.isfile(save_dir):
        # job 1. read given <input_file> as <fasta_str>
        with open(os.path.join(input_path, input_file)) as rdkit_input_file:  # like: Q99767.txt
            fasta_str = rdkit_input_file.readline()
        # job 2. convert read <fasta_str> in to 'Rdkit mol object'
        mymol = Chem.MolFromFASTA(fasta_str)
        # job 3. get mol object atom numbers into list variable
        atom_num_list=[]
        try:
            for atom in mymol.GetAtoms():
                atom_num_list.append(str(atom.GetAtomicNum()))
                #print(atom.GetAtomicNum())
            workerWriteListContentsToFileSingleLine(list_content=atom_num_list,save_dir=save_dir)
            # job 5. also write process descriptives in to disk
        except AttributeError:
            print('An error has occured on file %s...'%(input_file))
    else:
        print('%s file exists so nothing newly done...'%(save_file_name))
    return None

def looperConvertGivenRdKitInputToAtomicNums(input_dirs=None,output_dir=None,process_type=None,log_dir=None):
    processed_dirs = []
    # job 0. init loop files part
    for input_dir in input_dirs:
        sub_folder_name=input_dir.split('/')[-1]
        is_process_step_done = helperCheckLogFileAndGivenProcessLogRecordExistance(log_path=log_dir,log_file=process_type,process_name=sub_folder_name)
        print(sub_folder_name)
        process_dir = os.path.join(output_dir, sub_folder_name)
        processed_dirs.append(process_dir)
        if not is_process_step_done:
            # job 1. create given <process_dir> if not exists
            if not os.path.isdir(process_dir):
                os.mkdir(process_dir)
            # loop over given input dir files
            for job_file in os.listdir(input_dir): # .txt files which holds fasta strings
                workerConvertGivenRdKitInputToAtomicNums(input_path=input_dir,input_file=job_file,file_ext=process_type,output_dir=process_dir)
                print(job_file)
            # job 2. write log record for given process
            helperWriteFinishedProcessStepToLog(log_path=log_dir, log_file=process_type, log_contents=sub_folder_name)
        else:
            print('%s process is done already so skipping it...'%(sub_folder_name))

    return processed_dirs

def pipeHelperConvertGivenRdKitInputToAtomicNums(input_dirs=None,file_path=None,process_type='atom'):
    # job 0. get input dirs from previous step which is given by <input_dirs> argument
    process_input_dirs=input_dirs
    # job 1. initialize the job folders and paths
    process_dirs=helperInitFoldersAndPathsForGivenJob(job_dir=file_path,job_folders=FASTA_INPUT_JOB_FOLDERS)
    # job 2. detect function related output folder from given <process_dirs> argument by using <process_type> argument
    output_root_dir=helperFindElementFromGivenListByEndsWithKey(search_list=process_dirs, ends_with_key=process_type)
    log_root_dir = helperFindElementFromGivenListByEndsWithKey(search_list=process_dirs, ends_with_key='log')
    processed_dirs=looperConvertGivenRdKitInputToAtomicNums(input_dirs=process_input_dirs,output_dir=output_root_dir,process_type=process_type,log_dir=log_root_dir)

    return processed_dirs

### BUILDING ATOM INPUT FILES DESCRIPTIVES PART
def helperGetWavelengthCountForGivenAtom(data_path=None,atom_number=None,file_ext='wldat'):
    data_file='%s.%s'%(atom_number,file_ext)
    num_lines = sum(1 for line in open(os.path.join(WAVELENGTH_DB_DATA_PATH, data_file)))
    #print(num_lines)
    return num_lines

def helperConvertOneLinedFileIntoList(data_path=None,data_file=None,split_char=','):
    line_contents_holder=[]
    with open(os.path.join(data_path,data_file)) as input_file:
        for line in input_file.readlines():
            list_data=line.split(split_char)
            line_contents_holder=line_contents_holder+list_data

    return line_contents_holder

def helperWriteGivenProcessOutputsAsCsv(output_dir=None,process_output=None,col_names=['File_ID','Total_Points','File_Dir']):
    # job 0. detect file existane on disk if not write first line with <col_names> as a header
    if not os.path.isfile(output_dir):
        with open (output_dir,'w+') as f:
            f.write(','.join(col_names)+'\n')
            f.write(','.join(process_output) + '\n')
    else:
        with open (output_dir,'a') as f:
            f.write(','.join(process_output)+'\n')
    return None

def workerCalculateTotalCountOfSpiralPointsForGivenFile(file_path=None,file_name=None,save_dir=None):
    # job 0. declare the calculation variable
    total_spiral_points=0
    # job 1. convert given <atom> file content to list
    atom_list=helperConvertOneLinedFileIntoList(data_path=file_path, data_file=file_name)
    # job 2. loop ove list to make calculation
    for atom_item in atom_list:
        line_count=helperGetWavelengthCountForGivenAtom(data_path=WAVELENGTH_DB_DATA_PATH,atom_number=int(atom_item))
        total_spiral_points+=line_count
    output_values=[file_name.split('.')[0],str(total_spiral_points),os.path.join(file_path,file_name)]
    print(output_values)
    # job 3. write given file outputs to file
    helperWriteGivenProcessOutputsAsCsv(output_dir=save_dir,process_output=output_values)
    return None

def looperCalculateTotalCountOfSpiralPointsForGivenDir(input_dirs=None,output_dir=None,process_type=None,log_dir=None):
    processed_dirs = []
    # job 0. init loop files part
    for input_dir in input_dirs:
        out_file_name=input_dir.split('/')[-1]+'.csv' # we want csv files as output
        is_process_step_done = helperCheckLogFileAndGivenProcessLogRecordExistance(log_path=log_dir,log_file=process_type,process_name=out_file_name)
        print(out_file_name)
        process_file_dir = os.path.join(output_dir, out_file_name) # this is our file
        processed_dirs.append(process_file_dir)
        if not is_process_step_done:
            # job 1. loop over files in given <input_dir>
            for job_file in os.listdir(input_dir): # .txt files which holds fasta strings
                workerCalculateTotalCountOfSpiralPointsForGivenFile(file_path=input_dir, file_name=job_file, save_dir=process_file_dir)
                print(job_file)
            # job 2. write log record for given process
            helperWriteFinishedProcessStepToLog(log_path=log_dir, log_file=process_type, log_contents=out_file_name)
        else:
            print('%s process is done already so skipping it...'%(out_file_name))

    return processed_dirs

def pipeHelperCalculateTotalCountOfSpiralPointsForGivenDir(input_dirs=None,file_path=None,process_type='csv'):
    # job 0. get input dirs from previous step which is given by <input_dirs> argument
    process_input_dirs=input_dirs
    # job 1. initialize the job folders and paths
    process_dirs=helperInitFoldersAndPathsForGivenJob(job_dir=file_path,job_folders=FASTA_INPUT_JOB_FOLDERS)
    # job 2. detect function related output folder from given <process_dirs> argument by using <process_type> argument
    output_root_dir=helperFindElementFromGivenListByEndsWithKey(search_list=process_dirs, ends_with_key=process_type)
    log_root_dir = helperFindElementFromGivenListByEndsWithKey(search_list=process_dirs, ends_with_key='log')
    processed_dirs=looperCalculateTotalCountOfSpiralPointsForGivenDir(input_dirs=process_input_dirs,output_dir=output_root_dir,process_type=process_type,log_dir=log_root_dir)


    return processed_dirs

### FILTERING ATOM INPUT FILES DESCRIPTIVES PART
def helperCalculateMaxSpiralPointsToVisualize(max_img_size=None):
    import math
    max_points=int(math.pi*(max_img_size/2)**2)
    print(max_points)

    return max_points

def looperFilterOnlyVisualizableFilesForGivenDir(input_dirs=None,output_dir=None,process_type=None,log_dir=None):
    import pandas as pd
    processed_dirs = []
    # job 0. calculate the filter value
    #max_points_to_plot=helperCalculateMaxSpiralPointsToVisualize(max_img_size=MAX_IMG_SIZE)
    max_points_to_plot=620000 # currently manual given (may be look for it on a spare time)
    # job 1. init loop files part
    for input_dir in input_dirs: # <input_dir> like: '/media/burak/yedek/emission_job/external_data/uniclust50_2018_08/inputs/csv/uniclust50_2018_08_seed_000.csv'
        save_file_name = input_dir.split('/')[-1] # we want csv files as output like: 'uniclust50_2018_08_seed_000.csv'
        is_process_step_done = helperCheckLogFileAndGivenProcessLogRecordExistance(log_path=log_dir, log_file=process_type, process_name=save_file_name)
        process_file_dir = os.path.join(output_dir, save_file_name) # this is our file
        processed_dirs.append(process_file_dir)
        if not is_process_step_done:
            df=pd.read_csv(input_dir)
            df_filter=df[df['Total_Points']<=max_points_to_plot]
        # job 2. write filtered dataframe in disk
            save_dir=os.path.join(output_dir,save_file_name)
            #print(save_dir)
            df_filter.to_csv(save_dir,index=False)
        # job 3. write log record as descriptive info about how many filtered and total file for processed folders
            output_vals=[save_file_name,str(len(df)),str(len(df_filter))]
            descriptive_file_dir=os.path.join(log_dir,'input_descriptives.csv')
            helperWriteGivenProcessOutputsAsCsv(output_dir=descriptive_file_dir,process_output=output_vals,col_names=['Folder_Name','Total_File','Filtered_File'])
            #print(output_vals)
        # job 4. write pipe log record for given process
            helperWriteFinishedProcessStepToLog(log_path=log_dir, log_file=process_type, log_contents=save_file_name)
        else:
            print('%s process is done already so skipping it...' % (save_file_name))
    return processed_dirs

def pipeHelperFilterOnlyVisualizableFilesForGivenDir(input_dirs=None,file_path=None,process_type='filtered_csv'):
    # job 0. get input dirs from previous step which is given by <input_dirs> argument
    process_input_dirs=input_dirs
    # job 1. initialize the job folders and paths
    process_dirs=helperInitFoldersAndPathsForGivenJob(job_dir=file_path,job_folders=FASTA_INPUT_JOB_FOLDERS)
    # job 2. detect function related output folder from given <process_dirs> argument by using <process_type> argument
    output_root_dir=helperFindElementFromGivenListByEndsWithKey(search_list=process_dirs, ends_with_key=process_type)
    log_root_dir = helperFindElementFromGivenListByEndsWithKey(search_list=process_dirs, ends_with_key='log')
    processed_dirs=looperFilterOnlyVisualizableFilesForGivenDir(input_dirs=process_input_dirs,output_dir=output_root_dir,process_type=process_type,log_dir=log_root_dir)
    print(processed_dirs)
    return processed_dirs

### VISUALIZATION OF FILTERED INPUT FILES PART
def helperAdjustedRounder(input_value=None): # ondalık basamağı dikkate alarak en yakın tamsayıya yuvarlar
    rounded_value=int(input_value // 1 + ((input_value % 1) / 0.5) // 1)
    return rounded_value

def helperCalculateImageSizeForGivenPointsValue(total_points=None):
    import math
    image_size=helperAdjustedRounder(input_value=math.sqrt(total_points/math.pi))
    return image_size

# begin of workerDefineColorAndPositionInputsForSpiralPlot functions
def helperPandasToDict(input_path=None,input_file=None,search_key=None):
    import pandas as pd
    data_df=pd.read_csv(os.path.join(input_path,input_file))
    data_dict=dict(data_df.values)
    # arrange search_key_val input
    search_key=search_key.rsplit('.',1)[0] # he rsplit tells Python to perform the string splits starting from the right of the string, and the 1 says to perform at most one split (so that e.g. 'foo.bar.baz' -> ['foo.bar', 'baz'])
    # get given <search_key_val> value
    search_key_value=data_dict.get(search_key)
    return search_key_value

def helperReadOnlyColoredPixelsRGBForGivenImage(image_path=None,image_file=None):
    black_color=(0, 0, 0, 255)
    from PIL import Image
    photo = Image.open(os.path.join(image_path,image_file), 'r') # 19_emission_spectrum.png
    pixel_values = []
    width = photo.size[0]  # define W and H
    height = photo.size[1]

    for y in range(0, 1):  # each pixel has coordinates
        for x in range(0, width):
            rgb_intv = []

            RGB = photo.getpixel((x, y))
            if not RGB==black_color:
                for item in RGB:
                    item = item / 255
                    rgb_intv.append(item)

                pixel_values.append(rgb_intv)
                # R,G,B = RGB  #now you can use the RGB value
    return pixel_values

def helperReadPixelsRGBForGivenImage(image_path=None,image_file=None):
    from PIL import Image
    photo = Image.open(os.path.join(image_path,image_file), 'r') # 19_emission_spectrum.png
    pixel_values = []
    width = photo.size[0]  # define W and H
    height = photo.size[1]

    for y in range(0, 1):  # each pixel has coordinates
        for x in range(0, width):
            rgb_intv = []

            RGB = photo.getpixel((x, y))
            for item in RGB:
                item = item / 255
                rgb_intv.append(item)

            pixel_values.append(rgb_intv)
            # R,G,B = RGB  #now you can use the RGB value
    return pixel_values

def helperRGBToHexConverter(rgb_code=None, rgbtype=1):
    """Converts RGB values in a variety of formats to Hex values.

     @param  vals     An RGB/RGBA tuple
     @param  rgbtype  Valid valus are:
                          1 - Inputs are in the range 0 to 1
                        256 - Inputs are in the range 0 to 255

     @return A hex string in the form '#RRGGBB' or '#RRGGBBAA'
    """

    if len(rgb_code)!=3 and len(rgb_code)!=4:
        raise Exception("RGB or RGBA inputs to RGBtoHex must have three or four elements!")
    if rgbtype!=1 and rgbtype!=256:
        raise Exception("rgbtype must be 1 or 256!")

    #Convert from 0-1 RGB/RGBA to 0-255 RGB/RGBA
    if rgbtype==1:
        rgb_code = [255*x for x in rgb_code]

    #Ensure values are rounded integers, convert to hex, and concatenate
    hex_string='#' + ''.join(['{:02X}'.format(int(round(x))) for x in rgb_code])
    return hex_string

def helperMatplotlibClearMemory():
    from matplotlib import pyplot as plt
    #usedbackend = matplotlib.get_backend()
    #matplotlib.use('Cairo')
    allfignums = plt.get_fignums()
    for i in allfignums:
        fig = plt.figure(i)
        fig.clear()
        plt.close( fig )
    #matplotlib.use(usedbackend)
    return None

def helperConvertGivenWaveLengthToRGB(wavelength_data=None, gamma=0.8):

    '''This converts a given wavelength of light to an
    approximate RGB color value. The wavelength must be given
    in nanometers in the range from 380 nm through 750 nm
    (789 THz through 400 THz).
    Based on code by Dan Bruton
    http://www.physics.sfasu.edu/astro/color/spectra.html
    '''

    wavelength = float(wavelength_data)
    if wavelength >= 380 and wavelength <= 440:
        attenuation = 0.3 + 0.7 * (wavelength - 380) / (440 - 380)
        R = ((-(wavelength - 440) / (440 - 380)) * attenuation) ** gamma
        G = 0.0
        B = (1.0 * attenuation) ** gamma
    elif wavelength >= 440 and wavelength <= 490:
        R = 0.0
        G = ((wavelength - 440) / (490 - 440)) ** gamma
        B = 1.0
    elif wavelength >= 490 and wavelength <= 510:
        R = 0.0
        G = 1.0
        B = (-(wavelength - 510) / (510 - 490)) ** gamma
    elif wavelength >= 510 and wavelength <= 580:
        R = ((wavelength - 510) / (580 - 510)) ** gamma
        G = 1.0
        B = 0.0
    elif wavelength >= 580 and wavelength <= 645:
        R = 1.0
        G = (-(wavelength - 645) / (645 - 580)) ** gamma
        B = 0.0
    elif wavelength >= 645 and wavelength <= 750:
        attenuation = 0.3 + 0.7 * (750 - wavelength) / (750 - 645)
        R = (1.0 * attenuation) ** gamma
        G = 0.0
        B = 0.0
    else:
        R = 0.0
        G = 0.0
        B = 0.0
    R *= 255
    G *= 255
    B *= 255
    return (int(R), int(G), int(B))

def workerDefineColorFromDataFile(data_path=None,data_file=None):
    # job 0. read color points (which refers to wavelengths) from file
    data_holder=[]
    with open(os.path.join(data_path,data_file),'r') as wldat_file:
        for line in wldat_file.readlines():
            data_holder.append(line.strip())
    #print(data_holder)
    # job 1. get spectrum color matches from read data
    color_hex_holder=[]
    for wavelength_data in data_holder:
        rgb_code=helperConvertGivenWaveLengthToRGB(wavelength_data=wavelength_data)
        hex_code_str=helperRGBToHexConverter(rgb_code=rgb_code,rgbtype=256)
        color_hex_holder.append(str(hex_code_str))
    #print(color_hex_holder)
    return color_hex_holder

def helperSpiralPointsCalculator(arc=1, separation=1, numpoints=None): # https://stackoverflow.com/questions/13894715/draw-equidistant-points-on-a-spiral
    import numpy as np
    import math
    """generate points on an Archimedes' spiral
    with `arc` giving the length of arc between two points
    and `separation` giving the distance between consecutive
    turnings
    - approximate arc length with circle arc at given distance
    - use a spiral equation r = b * phi
    """
    def p2c(r, phi):
        """polar to cartesian
        """
        return (r * math.cos(phi), r * math.sin(phi))

    sx = np.zeros(numpoints)
    sy = np.zeros(numpoints)

    # yield a point at origin
    #yield (0, 0)

    # initialize the next point in the required distance
    r = arc
    b = separation / (2 * math.pi)
    # find the first phi to satisfy distance of `arc` to the second point
    phi = float(r) / b
    while numpoints > 0:
        sx[numpoints-1],sy[numpoints-1]=p2c(r, phi) # yield p2c(r, phi)

        # sx[numpoints-1]=r*math.cos(phi)
        # sy[numpoints-1]=r*math.sin(phi)


        # advance the variables
        # calculate phi that will give desired arc length at current radius
        # (approximating with circle)
        phi += float(arc) / r
        r = b * phi
        numpoints = numpoints - 1
    print('----')
    total_point_num=len(sy) # len(sy)=len(sx) doesnt matter
    print(len(sy))
    return sx, sy, total_point_num

def workerDefineColorAndPositionInputsForSpiralPlot(input_file_dir=None,out_path=None,out_file=None,run_type='emission',color_type='only',define_type='from_data',size_type='match'): # run_type is: 'absorption' or 'emission'
    '''
    :param run_type: 'emission' or 'absorption' or 'emission_with_intensity'
    :param color_type: 'only' or 'all' (only refers to just colored pixels)
    :return:
    '''
    import pandas as pd
    import gc
    # job 0. read input values from file in disk according to given <job_file_name>
    job_path,job_file=input_file_dir.rsplit('/', 1) # convert given dir to <job_path> and <job_file>
    print(job_path,job_file)
    atom_list=helperConvertOneLinedFileIntoList(data_path=job_path,data_file=job_file)

    #atom_length=helperPandasToDict(input_path=PROCESS_OUTPUTS_PATH,input_file='rdkit_descriptives.csv',search_key=job_file)
    # job 1. detection of the run type from <run_type> argument
    spectrum_image_type = '%s_spectrum' % (run_type) # spec image type definition arguments
    # job 2. detect color values for every point according to <color_type> argument
    colors_holder=[]
    for atom_item in atom_list:
        if define_type=='from_image':
            spec_image_file='%s_%s.png' % (atom_item, spectrum_image_type)
            if color_type=='only':
                rgba_values = helperReadOnlyColoredPixelsRGBForGivenImage(image_path=SPECTRUM_IMAGES_PATH,image_file=spec_image_file)
            else:
                rgba_values = helperReadPixelsRGBForGivenImage(image_path=SPECTRUM_IMAGES_PATH,image_file=spec_image_file)
            # convert rgba to hex
            for rgba_value in rgba_values:
                colors_holder.append(helperRGBToHexConverter(rgb_code=rgba_value))
        elif define_type=='from_data':
            spec_data_file = '%s.wldat' % (atom_item)
            hex_str_codes=workerDefineColorFromDataFile(data_path=WAVELENGTH_DB_DATA_PATH, data_file=spec_data_file)
            for hex_str_code in hex_str_codes:
                colors_holder.append(hex_str_code)
    # # job 3. (optional) after loop process finished add white codes to given <job_file> to match image size for every file
    # if size_type=='match':
    #     max_dimension_file_name,max_dimension_val=helperGetMaxValueFromGivenColumn(data_path=PROCESS_OUTPUTS_PATH,data_file='spiral_descriptives_trimmed.csv',column_name='Total_Points')
    #     gc.collect()
    #     colors_holder=helperFillColorListWithGivenColorCode(color_list=colors_holder,max_val=max_dimension_val)
    # job 3. after loop process finished calculate x and y values for every point part
    points_holder_x, points_holder_y, total_point_count = helperSpiralPointsCalculator(numpoints=len(colors_holder))
    # job 4. write calculated points and rgb codes as pandas dataframe
    df = pd.DataFrame(list(zip(points_holder_x, points_holder_y, colors_holder)),
                      columns=['x_values', 'y_values', 'hex_codes'])
    # job 5. write pandas in disk as <csv> file
    df.to_csv(os.path.join(out_path,out_file), index=False)
    # job 6. memory free
    del colors_holder,points_holder_x,points_holder_y,df,hex_str_codes
    gc.collect()

    return total_point_count

def workerPlotSpiralFromData(data_path=None,data_file=None,save_path=None,save_file=None,total_points=None):
    from matplotlib import pyplot as plt
    from PIL import Image
    import pandas as pd
    import gc
    # job 0. detect image size part
    image_size=helperCalculateImageSizeForGivenPointsValue(total_points=total_points)
    print(image_size,total_points)
    # job 0. read disk data in memory part
    data_df=pd.read_csv(os.path.join(data_path,data_file))
    # job 1. building spiral spectrum image from data part
    fig, ax = plt.subplots(figsize=(image_size*9 / float(166), image_size*9 / float(166)), dpi=166) #fig, ax = plt.subplots(figsize=(100,100))

    ax.scatter(data_df['x_values'],data_df['y_values'], c=data_df['hex_codes'], s=0.1)
    #plt.show()
    # add an 'png' folder under given <save_path>
    if not os.path.isdir(os.path.join(save_path,'png')):
        os.mkdir(os.path.join(save_path,'png'))

    # remove x and y axis labels and ticks
    plt.axis('off')
    plt.savefig(os.path.join(save_path,'png',save_file), dpi=166)

    # add an 'jpg' folder under given <save_path>
    if not os.path.isdir(os.path.join(save_path,'jpg')):
        os.mkdir(os.path.join(save_path,'jpg'))
    # convert saved png format file to optimized jpeg
    im = Image.open(os.path.join(save_path,'png',save_file))
    im = im.convert('RGB')
    save_file_jpg='%s.jpg'%(save_file.split('.')[0])
    im.save(os.path.join(save_path,'jpg',save_file_jpg), quality=JPEG_QUALITY)
    # Clear the all the figures and check memory usage:
    helperMatplotlibClearMemory()
    # Free memory
    gc.collect()
    fig.clf()
    plt.close(fig)
    del fig,ax
    gc.collect()
    return None

def looperFastaSequenceVisualiser(input_dirs=None,image_output_path=None,data_output_path=None,log_dir=None):
    import pandas as pd
    import gc
    # job 0. init whole process by looping over <input_dirs>
    for input_dir in input_dirs:
        sub_folder_name=input_dir.split('/')[-1].split('.')[0] # like: 'uniclust50_2018_08_seed_000'
        is_process_step_done = helperCheckLogFileAndGivenProcessLogRecordExistance(log_path=log_dir,log_file='finished_folders',process_name=sub_folder_name)
        if not is_process_step_done:
        # job 1. our job files are in <input_dir> csv file related column so we must get them firstly
            job_files_df=pd.read_csv(input_dir)
            job_files=job_files_df['File_Dir'].values.tolist()
        # job 2. now we call an inner loop to walk in job_files to do the job
            progress_counter = 0
            for job_file in job_files: # job_file like: /media/burak/yedek/emission_job/external_data/uniclust50_2018_08/inputs/atom/uniclust50_2018_08_seed_000/A0A2R5G888.atom
                progress_counter += 1
                atom_input=os.path.basename(job_file)
                csv_input=atom_input.replace('atom','csv')
                output_file_name=csv_input.replace('csv','png')
                print(atom_input,csv_input,output_file_name)
                # job 3. create the necessary folders if not exists
                if not os.path.isdir(os.path.join(image_output_path,sub_folder_name)):
                    os.mkdir(os.path.join(image_output_path,sub_folder_name))
                    os.mkdir(os.path.join(data_output_path,sub_folder_name))
                # job 4. run image building process by checking if given file is not processed already
                if not os.path.isfile(os.path.join(image_output_path, sub_folder_name,'png',output_file_name)):
                    # visualising part
                    print('%s/%s %s file is visualising...' % (progress_counter, len(job_files), atom_input))
                    csv_output_path=os.path.join(data_output_path,sub_folder_name)
                    png_output_path=os.path.join(image_output_path,sub_folder_name)
                    total_point_count=workerDefineColorAndPositionInputsForSpiralPlot(input_file_dir=job_file,out_path=csv_output_path,out_file=csv_input)
                    workerPlotSpiralFromData(data_path=csv_output_path, data_file=csv_input,save_path=png_output_path,save_file=output_file_name,total_points=total_point_count)
                else:
                    print('%s/%s visualised version of %s file exists so skipping...' % (progress_counter, len(job_files), atom_input))
                #break
            # job 5. write finished folder contents log record
            helperWriteFinishedProcessStepToLog(log_path=log_dir, log_file='finished_folders', log_contents=sub_folder_name)
        else:
            print('%s folder contents is done already so skipping it...' % (sub_folder_name))
    return None

def pipeHelperFastaSequenceVisualiser(input_dirs=None,file_path=None,output_dirs=None):
    # job 0. initialize the job folders and paths (these are the output folders)
    process_dirs=helperInitFoldersAndPathsForGivenJob(job_dir=file_path,job_folders=output_dirs)
    # job 2. detect function related output folder from given <process_dirs> argument by using <process_type> argument
    output_image_root_dir=helperFindElementFromGivenListByEndsWithKey(search_list=process_dirs, ends_with_key='image')
    output_data_root_dir = helperFindElementFromGivenListByEndsWithKey(search_list=process_dirs, ends_with_key='data')
    log_root_dir = helperFindElementFromGivenListByEndsWithKey(search_list=process_dirs, ends_with_key='log')
    looperFastaSequenceVisualiser(input_dirs=input_dirs, image_output_path=output_image_root_dir, data_output_path=output_data_root_dir, log_dir=log_root_dir)
    return None

### MAIN FUNCTION DEFINITION (DESIGNED FOR PIPE RUN)
def pipeCallerInputDataBuilderForEmissionImages(input_path=None,input_file=None):
    # step 1. convert multiple fasta files to txt based fasta records part
    next_step_input_dirs=pipeHelperSplitFastaFileForRdkitInput(file_path=input_path, file_name=input_file)
    # step 2.
    next_step_input_dirs=pipeHelperConvertGivenRdKitInputToAtomicNums(input_dirs=next_step_input_dirs,file_path=input_path)
    # step 3.
    next_step_input_dirs=pipeHelperCalculateTotalCountOfSpiralPointsForGivenDir(input_dirs=next_step_input_dirs,file_path=input_path)
    # step 4.
    next_step_input_dirs=pipeHelperFilterOnlyVisualizableFilesForGivenDir(input_dirs=next_step_input_dirs, file_path=input_path, process_type='filtered_csv')
    # step 5.
    pipeHelperFastaSequenceVisualiser(input_dirs=next_step_input_dirs, file_path=input_path, output_dirs=FASTA_OUTPUT_JOB_FOLDERS)
    return None

### CALL THE MAIN FUNCTION
pipeCallerInputDataBuilderForEmissionImages(input_path=FASTA_DATA_PATH,input_file=FASTA_INPUT_LIST_FILE)
