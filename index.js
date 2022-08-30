const fs = require("fs")
const path = require("path")
let { S3Client, PutObjectCommand } = require("@aws-sdk/client-s3")

const console = global.console

let conf = {}
let bucket = undefined
let client = undefined
let base_s3_path = undefined
let base_file_path = undefined
let s3_counter = 0

function read_conf()
{
    try
    {
        conf = require(path.resolve(".s3pub"))

        //black list
        if (conf.excludes)
        {
            let excludes = {}
            conf.excludes.forEach((one) =>
            {
                excludes[one] = true
            })

            conf.excludes = excludes
        }

        conf.nest = conf.nest || 20
        conf.batch = conf.batch || 20
    }
    catch (e)
    {
        console.error(e)
        throw new Error(".s3pub.json file must be given in root path. see README.md.")
    }

    return conf
}

function log(...args)
{
    if (conf.logoff)
        return

    console.log(...args)
}

function is_in_backlist(file)
{
    if (!conf.excludes)
        return false

    file = path.basename(file)
    return conf.excludes[file]
}

function check_pri(file)
{
    if (!conf.files_pri)
        return

    let relative = path.relative(base_file_path, file)
    return conf.files_pri[relative]
}

function sleep(ms)
{
    return new Promise((resolve, reject) =>
    {
        setTimeout(resolve, ms)
    })
}

async function __s3_put(file_path)
{
    let s3_path = undefined
    if (conf.remove_prefix)
    {
        let relative = path.relative(base_file_path, file_path)
        relative = relative || file_path //self

        s3_path = `${base_s3_path}${relative}`
    }
    else
    {
        s3_path = `${base_s3_path}${file_path}`
    }

    //normalize
    s3_path = s3_path.replace(/\\/g, '/')

    let meta_data = {}

    //headers inject
    if (conf.headers)
    {
        let relative = path.relative(base_file_path, file_path)
        let headers = conf.headers[relative] || conf.headers.default
        if (headers)
        {
            Object.assign(meta_data, headers)
        }
    }

    let ContentType = undefined
    let ext = path.extname(file_path)
    if (conf.mime && conf.mime.types[ext])
    {
        log(`ext:${ext}, file_path:${file_path}`)
        ContentType = conf.mime.types[ext]
    }

    let retry = 0
    let ok = false
    while (!ok && retry < 3)
    {
        try
        {
            const fileStream = fs.createReadStream(file_path)
            const uploadParams = {
                Bucket: bucket,
                // Add the required 'Key' parameter using the 'path' module.
                Key: s3_path,
                // Add the required 'Body' parameter
                Body: fileStream,
                Metadata: meta_data,
                ContentType
            }

            let meta_cache_control = meta_data["CacheControl"] || meta_data["cache-control"]
            if (meta_cache_control)
                uploadParams.CacheControl = meta_cache_control

            await client.send(new PutObjectCommand(uploadParams))
            ok = true
        }
        catch (e)
        {
            global.console.error(`pushing file[${file_path}]`, e)
            ++retry
            global.console.error(`file[${file_path}] will retry`)
        }
    }
    if (!ok)
        throw new Error(`push file:[${file_path}] fail`)

    ++s3_counter
    log(`pushed file:[${file_path}] to s3:[${s3_path}]`)
}

function s3_put(file_path, pros, white_pris = [], ignore_pri = true, nest = 0)
{
    ++nest

    if (is_in_backlist(file_path))
    {
        log(`skip file due to the blacklist:${file_path}`)
        return
    }

    if (!ignore_pri)
    {
        let pri = check_pri(file_path)
        if (pri != null)
        {
            white_pris.push({ file: file_path, pri })
            return
        }
    }

    let st = fs.statSync(file_path)
    if (st.isFile())
    {
        pros.push(__s3_put.bind(undefined, file_path));
        return
    }
    else if (st.isDirectory())
    {
        if (nest > conf.nest)
        {
            return
        }

        let files = fs.readdirSync(file_path);
        files.forEach(
            (file) =>
            {
                let sub_file_path = `${file_path}/${file}`
                s3_put(sub_file_path, pros, white_pris, ignore_pri, nest)
            }
        )
    }
}

async function exec_pros(pros)
{
    let batch = conf.batch
    while (pros.length > 0)
    {
        let batch_pros = pros.splice(0, batch)
        batch_pros = batch_pros.map((one) =>
        {
            return one()
        })
        await Promise.all(batch_pros)
    }
}

async function exec()
{
    let pros = []
    let wait_files = []
    s3_put(base_file_path, pros, wait_files, false)

    //throw the error 
    await exec_pros(pros)

    wait_files.sort((a, b) =>
    {
        return b.pri - a.pri
    })

    //upload one by one
    for (let one of wait_files)
    {
        let sub_pros = []
        s3_put(one.file, sub_pros)

        await exec_pros(sub_pros)
    }
}

async function main()
{
    read_conf()

    let { accessKeyId, secretAccessKey } = conf
    if (!accessKeyId)
    {
        throw new Error("accessKeyId must be given in configuration. see README.md.")
    }

    if (!secretAccessKey)
    {
        throw new Error("secretAccessKey must be given in configuration. see README.md.")
    }

    let region = process.argv[2]
    bucket = process.argv[3]
    base_s3_path = process.argv[4]
    base_file_path = process.argv[5]
    if (!region || !bucket || !base_file_path || !base_s3_path)
    {
        throw new Error("invalid arguments count. usage: s3pub ${REGION} ${BUCKET_NAME} ${OSS_PATH} ${LOCAL_PATH}")
    }

    let endpoint = `https://s3.${region}.amazonaws.com` //通用的
    client = new S3Client({
        region,
        endpoint,
        credentials:
        {
            accessKeyId,
            secretAccessKey
        }
    });

    console.log(`pushing s3, region:${region}, bucket:${bucket}`)
    console.log(`=====================================================`)

    let start = Date.now()

    // base_s3_path = path.normalize(base_s3_path)
    if (base_s3_path == "/")
        base_s3_path = ""

    base_file_path = path.normalize(base_file_path)
    await exec()

    let stop = Date.now()
    console.log(`=====================================================`)
    console.log(`push s3 finish. files count:${s3_counter}, use_time:${(stop - start) / 1000} secs`)
    s3_counter = 0
}

main()
