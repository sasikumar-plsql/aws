import os
import json
import boto3

def lambda_handler(event, context):
    
    src_bucket='tutorialsasi'
    tgt_bucket='targetcopy'
    
    sc=boto3.client('s3')
    sr=boto3.resource('s3')
    prefix='app/flat_files/'
    
    #specific files to verify and copy...
    file_names=['books.xml','new_industry_sic.csv']
    
    try:
        for pfx in file_names:
            print(f"file name :  {pfx}")
            
            pfx=prefix+pfx
            
            List objects in the specified bucket with specified prefix
            prefix is specific sub folder search...
            
            response = sc.list_objects_v2(Bucket=src_bucket,Prefix=prefix)
            
            create list of file keys from response contents...
            file_keys = [obj['Key'] for obj in response['Contents']]
            print(f"file keys : {file_keys}")
            
            #verify file found matching the file key...
            if pfx in file_keys:
                print(f"found : {pfx}")
                source={'Bucket' :  src_bucket, 'Key' : pfx}
                dest_key="backup/flatfiles/"+os.path.basename(pfx)
                print(source)
                print(dest_key)
                print("\n")
                
                #copy source file into target bucket/folder...
                sr.meta.client.copy(source, tgt_bucket, dest_key)
            else:
                #match file not found in the file key...
                print('file not found')
            
    except Exception as e:
        #for all other exception raise here
        #return 500 error code...
        print(f"Error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {e}")
        }
    finally:
        after smooth execution of program
        return success with 200 status code...
        return {
            'statusCode': 200,
            'body': json.dumps('file copy process completed...')
        }
