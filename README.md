# FfmEncoder
convert DataFrame to libffm data format in parallel

# Usage

    filed_names = [field1, field2, ... fieldn]
    fe = FfmEncoder(filed_names, nthread=10)
    fe.transform(train_X, 'train.ffm')
  
notice: The label column must be the first column in the input DataFrame
