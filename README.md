# FfmEncoder
convert DataFrame to libffm data format in parallel

# Usage

    filed_names = [field1, field2, ... fieldn]
    fe = FfmEncoder(filed_names,label_name='label',nthread=18)
    fe.transform(train_set, 'train.ffm')
  
~~notice: The label column must be the first column in the input DataFrame~~
