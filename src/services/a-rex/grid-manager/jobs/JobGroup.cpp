#include <cstddef>
#include <ctime>
#include <exception>
#include <fstream>
#include <list>
#include <mutex>
#include <ostream>
#include <regex>
#include <sigc++/functors/functor_trait.h>
#include <string>
#include <sys/stat.h>
// #include <system_error>
#include <filesystem>

#include <arc/GUID.h>
#include "GMJob.h"

#include "../files/ControlFileContent.h"
#include "../files/ControlFileHandling.h"
#include "../../delegation/DelegationStore.h"
#include "../../delegation/DelegationStores.h"
#include "../conf/GMConfig.h"
#include "../../job.h"
#include "JobsList.h"

#include "JobGroup.h"

namespace fs = std::filesystem;

namespace IFIC {

std::map<std::string, std::unique_ptr<JobGroup>> JobGroups::groups = std::map<std::string, std::unique_ptr<JobGroup>>();
static Arc::Logger logger = Arc::Logger(Arc::Logger::getRootLogger(), "IFIC.JobGrouping", Arc::DEBUG);
std::mutex JobGroups::_lock;

//
// Utilities
//
template<typename T>
const std::string printList(std::list<T>& list, const std::string& (* getter)(const T&)){
    std::string values = "[";

    for(typename std::list<T>::const_iterator it = list.begin(); it != list.end(); ++it)
        values += " " + getter(*it) + ",";

    if(values.length() > 1)
        values.pop_back();

    values += " ]";
    return values;
};

const std::string getJLDValue(const ARex::JobLocalDescription& jld, std::string key){
    if(key == "queue") return jld.queue;
    if(key == "node") return jld.headnode;
    if(key == "host") return jld.headhost;
    if(key == "DN") return jld.DN;
    if(key == "client_name") return jld.clientname;
    if(key == "job_name") return jld.jobname;
    return "";
};

std::string GetDelegationId(const ARex::GMConfig& config, std::string senderPEMLocation, std::string senderDn){
    ARex::DelegationStores* delegs = config.GetDelegations();
    if (!delegs) {
        logger.msg(Arc::FATAL, "Failed to obtain delegations");
        return "";
    }

    ARex::DelegationStore& store = (*delegs)[config.DelegationDir()];

    std::ifstream pemFile(senderPEMLocation);
    std::stringstream buffer;

	buffer << pemFile.rdbuf();

    std::string credentials_pem = buffer.str();

    std::string deleg_id;  

    if (!store.AddCred(deleg_id, senderDn, credentials_pem)) {
        logger.msg(Arc::FATAL, "Failed to store credential for job group");
        return "";
    }

    return deleg_id;
};

std::string ParseLocalDescription(ARex::JobLocalDescription* localDesc){
    std::string parsed;

    parsed += "- Job Id: "      + localDesc->jobid + "\n";
    parsed += "- Job Name: "    + localDesc->jobname + "\n";
    parsed += "- Global Id: "   + localDesc->globalid + "\n";
    parsed += "- Head Node: "   + localDesc->headnode + "\n";
    parsed += "- Head Host: "   + localDesc->headhost + "\n";
    parsed += "- Global URL: "  + localDesc->globalurl + "\n";
    parsed += "- Interface: "   + localDesc->interface + "\n";
    parsed += "- LRMS: "        + localDesc->lrms + "\n";
    parsed += "- Queue: "       + localDesc->queue + "\n";
    parsed += "- Local Id: "    + localDesc->localid + "\n";
    
    parsed += "- Exec:";
    for (auto str: localDesc->exec)
        parsed += " " + str;
    parsed += "\n";
    
    parsed += "- DN: "              + localDesc->DN + "\n";
    parsed += "- Start Time: "      + localDesc->starttime.str(Arc::TimeFormat::UserTime) + "\n";
    parsed += "- Life Time: "       + localDesc->lifetime + "\n";
    parsed += "- Process Time: "    + localDesc->processtime.str(Arc::TimeFormat::UserTime) + "\n";
    parsed += "- Exec Time: "       + localDesc->exectime.str(Arc::TimeFormat::UserTime) + "\n";
    parsed += "- Notify: "          + localDesc->notify + "\n";
    parsed += "- Client Name: "     + localDesc->clientname + "\n";
    parsed += "- Software: "        + localDesc->clientsoftware + "\n";
    parsed += "- Delegation Id: "   + localDesc->delegationid + "\n";
    
    int reruns = -1;
    if (localDesc->reruns)
        reruns = localDesc->reruns;
    parsed += "- Reruns: "          + std::to_string(reruns) + "\n";

    int priority = -1;
    if (localDesc->priority)
        priority = localDesc->priority;
    parsed += "- Priority: "        + std::to_string(priority) + "\n";

    int downloads = -1;
    if (localDesc->downloads)
        downloads = localDesc->downloads;
    parsed += "- Donwloads: "       + std::to_string(downloads) + "\n";
    
    int uploads = -1;
    if (localDesc->uploads)
        uploads = localDesc->uploads;
    parsed += "- Uploads: "         + std::to_string(uploads) + "\n";
    
    parsed += "- Session Dir: "     + localDesc->sessiondir + "\n";
    
    parsed += "- Project Names: ";
    for (auto str: localDesc->projectnames)
        parsed += " " + str;
    parsed += "\n";
    
    parsed += "- Job Report: ";
    for (auto str: localDesc->jobreport)
        parsed += " " + str;
    parsed += "\n";
    
    parsed += "- Local VO:";
    for (auto str: localDesc->localvo)
        parsed += " " + str;
    parsed += "\n";
    
    parsed += "- Voms:";
    for (auto str: localDesc->voms)
        parsed += " " + str;
    parsed += "\n";

    parsed += "- Auth Groups:";
    for (auto str: localDesc->authgroups)
        parsed += " " + str;
    parsed += "\n";
    
    parsed += "- Input Data:\n";
    for (auto file: localDesc->inputdata){
        parsed += "\n > PFN: " + file.pfn + "\n";
        parsed += "\n > LFN: " + file.lfn + "\n";
        parsed += "\n > Cred: " + file.cred + "\n";
        parsed += "\n > Cred Type: " + file.cred_type + "\n";
    }
    
    parsed += "- Output Data:\n";
    for (auto file: localDesc->outputdata){
        parsed += "\n > PFN: " + file.pfn + "\n";
        parsed += "\n > LFN: " + file.lfn + "\n";
        parsed += "\n > Cred: " + file.cred + "\n";
        parsed += "\n > Cred Type: " + file.cred_type + "\n";
    }
    
    parsed += "- RTEs:";
    for (auto str: localDesc->rte)
        parsed += " " + str;
    parsed += "\n";
    
    parsed += "- Action: "  + localDesc->action + "\n";
    parsed += "- STDin: "   + localDesc->stdin_ + "\n";
    parsed += "- STDout: "  + localDesc->stdout_ + "\n";
    parsed += "- STDerr: "  + localDesc->stderr_ + "\n";

    return parsed + "\n";
};

std::list<std::string> CheckRegex(std::string content, std::string regexStr){
    std::list<std::string> retVal;

    std::regex regex(regexStr);

    std::sregex_iterator regex_begin = 
        std::sregex_iterator(content.begin(), content.end(), regex);
    std::sregex_iterator regex_end = std::sregex_iterator();

    for (std::sregex_iterator i = regex_begin; i != regex_end; ++i){
        std::smatch match = *i;
        std::string match_str = match.str();
        
        retVal.push_back(match_str);
    }

    if (!retVal.empty())
        logger.msg(
            Arc::DEBUG, "Regex pattern '%s' Found: %s", 
            regexStr, 
            printList<std::string>(retVal, [](const std::string& c) -> const std::string& { return c; })
        );
    else
        logger.msg(Arc::DEBUG, "Regex pattern '%s' found nothing", regexStr);

    return retVal;
};

std::string BuildFileDesc(ARex::FileData file){
    std::string location;

    if (file.has_lfn())
        location = file.lfn;
    else
        location = file.pfn;
    
    std::string fileBase = basename(file.lfn.c_str());

    return "(\"" + fileBase + "\" \"" + location + "\")\n";
};

std::list<std::string> CutRegex(std::list<std::string> matches, char splitChar, int trimBack=0, int trimFront=0){
    std::list<std::string> newMatches; 
    for(std::string entry : matches){
        size_t pos = entry.find(splitChar);
        std::string modifiedEntry = entry.substr(pos+1+trimBack, entry.size()-pos-1-trimBack-trimFront);
        newMatches.push_back(modifiedEntry);
    }
    return newMatches;
}

//
// JLDChecker
//
bool JLDChecker::isValid(const ARex::JobLocalDescription& jobRef){
    logger.msg(
        Arc::DEBUG,
         "Checking job '%s' following the condition: '%s' '%s' '%s'",
          jobRef.jobid, by, condition, check_value);

    const std::string var = IFIC::getJLDValue(jobRef, by);

    if (var == ""){
        logger.msg(Arc::WARNING, "Var '%s' not found in JLD", by);
        return false;
    }

    if (condition == "eq")
        return var == check_value;

    if (condition == "neq")
        return var != check_value;

    if (condition == "in")
        return check_value.find(var) != std::string::npos;

    return false;
};

//
// GroupedJob
//
bool GroupedJob::AreInputsReady(const ARex::GMConfig& config){
    // Check control dir of job to populate the requirments
    if (!requirementsProcessed){
        logger.msg(Arc::DEBUG, "Processing grami file at %s", controlDir.generic_string());

        fs::path grami = fs::path(controlDir).append("grami");

        std::ifstream gramiStream(grami);

        std::stringstream gramiBuffer;
        gramiBuffer << gramiStream.rdbuf();
        std::string gramiContent = gramiBuffer.str();

        gramiStream.close();

        std::list<std::string> requiredInputNames_  = CutRegex(
            CheckRegex(gramiContent, "joboption_inputfile_[0-9]+='.*'"),
            '=', 1, 1);
        
        for(std::string input : requiredInputNames_){
            requiredInputNames.push_back(fs::path(input).filename().generic_string());
        }
        outputs = CutRegex(
            CheckRegex(gramiContent, "joboption_outputfile_[0-9]+='.*'"), 
            '=', 1, 1);
        envs    = CutRegex(
            CheckRegex(gramiContent, "joboption_env_[0-9]+='.*'"), 
            '=', 1, 1);
        RTEs    = CutRegex(
            CheckRegex(gramiContent, "joboption_runtime_[0-9]+(_[0-9]+)?='.*'"), 
            '=', 1, 1);
        
        requirementsProcessed = true;

        logger.msg(Arc::DEBUG, "Final info:");
        logger.msg(Arc::DEBUG, "Required Input Names: %s",
            printList<std::string>(
                requiredInputNames,
                [] (const std::string& c) -> const std::string& { return c; }));
        logger.msg(Arc::DEBUG, "Outputs: %s",
            printList<std::string>(
                outputs,
                [] (const std::string& c) -> const std::string& { return c; }));
        logger.msg(Arc::DEBUG, "Envs: %s",
            printList<std::string>(
                envs,
                [] (const std::string& c) -> const std::string& { return c; }));
        logger.msg(Arc::DEBUG, "RTEs: %s",
            printList<std::string>(
                RTEs,
                [] (const std::string& c) -> const std::string& { return c; }));
    };
    
    logger.msg(Arc::DEBUG, "Checking input files at workdir %s", workDir.generic_string());

    std::list<std::string> files;


    for (auto entry : fs::directory_iterator(workDir)){
        if (entry.is_regular_file()){
            std::string input = entry.path().filename().generic_string();
            logger.msg(Arc::DEBUG, "Found %s", input);
            files.push_front(input);
        }
    }

    size_t filesSize = files.size();
    size_t reqInputsSize = requiredInputNames.size();

    if (reqInputsSize != filesSize){
        logger.msg(Arc::DEBUG, "Not all inputs downloaded: %d/%d", (int)filesSize, (int)reqInputsSize);
        return false;
    }

    for (std::string reqInput : requiredInputNames){
        bool found = false;

        for(std::string foundInput : files){
            if(foundInput == reqInput){
                found = true;
                break;
            }
        }

        if(!found){
            logger.msg(Arc::FATAL, "Grouped inputs mismatch for file: %s", reqInput);
            logger.msg(Arc::FATAL, "Required files: %s", 
                printList<std::string>(
                    requiredInputNames,
                    [](const std::string& c) -> const std::string& { return c; }));
            logger.msg(Arc::FATAL, "Found files: %s",
                printList<std::string>(
                    files,
                    [](const std::string& c) -> const std::string& { return c; }));
            return false;
        }
        else
            files.remove(reqInput);
    }

    logger.msg(Arc::DEBUG, "All inputs are ready!");
    return true;
};

void GroupedJob::MoveInputs(const std::string& newDir, bool copyInstead){
    bool inputsFailed = false;

    for(const std::string& input : requiredInputNames){
        std::error_code ec;

        logger.msg(Arc::DEBUG, "Moving input '%s'", input);

        fs::path inputPath = fs::path(workDir).append(input);
        fs::path newLocation = fs::path(newDir)
                                .append(inputPath.filename().generic_string() + "_" + jobRef->get_id());

        logger.msg(Arc::DEBUG, "Moving/Copying file '%s' to '%s'", inputPath.generic_string(), newDir);

        if(copyInstead)
            fs::copy_file(inputPath, newLocation, ec);
        else
            fs::rename(inputPath, newLocation, ec);

        if (ec.value() != 0){
            logger.msg(Arc::FATAL, "Failed to move the inputs: EC(%d)", ec.value());
            inputsFailed = true;
        }
    }

    inputsMoved = ! inputsFailed;
};

//
// JobGroup
//
JobGroup::JobGroup(std::string name, const IFIC::GroupConfig config):
    name(name),
    checker(JLDChecker(config.by, config.condition, config.check_value)),
    max_jobs(config.max_jobs),
    max_seconds(config.max_seconds),
    senderDn(config.senderDn),
    senderPEMLocation(config.senderPEMLocation)
{
    std::string uuid = Arc::UUID();
    size_t pos = uuid.find_last_of("-");
    id = uuid.substr(pos+1);

    basePath = fs::path("/tmp")
                .append("groups")
                .append(id);

    inputPath = fs::path(basePath)
                    .append("inputs");

    mkdir(basePath.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    mkdir(inputPath.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
};

void IFIC::JobGroup::PrepareConfigFile(){
    std::string group_config;

    group_config = "export JOB_IDS = ( ";

    for(GroupedJob job : *this)
        group_config += job.jobRef->get_id() + " ";
    
    group_config += ")\n";

    group_config += "export BUNDLE_ID = " + id + "\n";

    std::ofstream file(fs::path(basePath).append("groupConfig"));

    file << group_config;
};

void JobGroup::SendGroup(ARex::JobsList* jobList, const ARex::GMConfig& config){
    std::string uuid = Arc::UUID();
    size_t pos = Arc::UUID().find_last_of("-");
    ARex::JobId id = uuid.substr(pos+1);
    logger.msg(Arc::DEBUG, "Group job id: %s", id);

    Arc::User owner = Arc::User(0); // TODO: SET OWNER

    std::string sessionDir = config.SessionRoot("") + '/' + id;

    ARex::GMJobRef job(new ARex::GMJob(id, owner, sessionDir, ARex::JOB_STATE_ACCEPTED));
    
    logger.msg(Arc::DEBUG, "Setting up local desc");

    ARex::JobLocalDescription* local = new ARex::JobLocalDescription();

    local->jobid = id;
    local->DN = senderDn;
    local->lrms = config.DefaultLRMS();  // TODO: NEEDED??
    local->queue = this->front().jobRef->GetLocalDescription()->queue;
    local->exec.push_back("/bin/true"); // TODO: THIS HAS TO BE THE EXEC THAT LAUNCHES THE MERGED JOBS
    local->sessiondir = sessionDir;
    
    logger.msg(Arc::DEBUG, "Setting inputs and outputs");
    local->inputdata = std::list<ARex::FileData>();
    local->outputdata = std::list<ARex::FileData>();

    // !!!! CRITICAL !!!!
    // TODO: CHECK REST OF LocalDescription PARAMETERS (SUCH AS RTEs)

    logger.msg(Arc::DEBUG, "Merging jlds");
    for (GroupedJob job : *this){
        ARex::JobLocalDescription* jobLocal = job.jobRef->GetLocalDescription();

        if (! jobLocal->inputdata.empty())
            local->inputdata.insert(
                local->inputdata.end(), 
                jobLocal->inputdata.begin(), 
                jobLocal->inputdata.end());

        if (! jobLocal->outputdata.empty())
            local->outputdata.insert(
                local->outputdata.end(), 
                jobLocal->outputdata.begin(), 
                jobLocal->outputdata.end());
    }

    logger.msg(Arc::DEBUG, "Obtaining delegation");
    ARex::DelegationStores* delegs = config.GetDelegations();
    if (!delegs) {
        logger.msg(Arc::FATAL, "Failed to obtain delegations");
        return;
    }

    std::string deleg_id = GetDelegationId(config, senderPEMLocation, senderDn);

    if (deleg_id == "") {
        return;
    }

    local->delegationid = deleg_id;
    

    if (! config.JobGroups(name)->dry_run) {
        // std::error_code e;

        // std::filesystem::create_directory(sessionDir, e);
        // if (e.value() != 0)
        //     logger.msg(Arc::ERROR, "Failed to create job directory, error: %s", e.value());
        

        // std::filesystem::create_directory(config.ControlDir() + id, e);
        // if (e.value() != 0)
        //     logger.msg(Arc::ERROR, "Failed to create job directory, error: %s", e.value());
        
        // ARex::job_local_write_file(*job, config, *local);

        // fs::path baseDir(local->sessiondir);
        // fs::path emptyFile("GROUPED");
        // std::ofstream file(baseDir / emptyFile);

        // logger.msg(Arc::INFO, "Requesting attention of group '%s'", name);
        // jobList->RequestAttention(job);
    }
    else {
        logger.msg(
            Arc::INFO, "DryRun activated for group %s", 
            name
        );
        logger.msg(
            Arc::INFO, 
            "Would have sent job (%s) with delegationId (%s) to queue (%s)",
            id, local->delegationid, local->queue 
        );
        logger.msg(
            Arc::INFO, 
            "Job (%s) has '%s' as executable, '%s' as inputs and '%s' as outputs",
            id,
            printList<std::string>(
                local->exec, 
                [](const std::string& c) -> const std::string&{ return c; }),
            printList<ARex::FileData>(
                local->inputdata, 
                [](const ARex::FileData& c) -> const std::string& { return c.lfn; }),
            printList<ARex::FileData>(
                local->outputdata, 
                [](const ARex::FileData& c) -> const std::string& { return c.lfn; })
        );
    }

    firstJobTime = std::chrono::steady_clock::time_point{};
};

void JobGroup::SendExec(const ARex::GMConfig& config){
    // General Info
    PrepareConfigFile();

    logger.msg(Arc::VERBOSE, "Building XLRS");
    
    logger.msg(Arc::DEBUG, "Job Descriptions of Group");
    
    for (auto job : *this)
        logger.msg(Arc::DEBUG, ParseLocalDescription(job.jobRef->GetLocalDescription()));
    
    logger.msg(Arc::DEBUG, "> Building general info");
    
    std::string desc_str =
        "&(executable=/arc/launch_group.sh)\n"
        "(jobname=grouped_" + getId() + ")\n"
        "(stdout=std.out)\n"
        "(stderr=std.err)\n";
    
    // Inputs
    logger.msg(Arc::DEBUG, "> Building input files");
    desc_str += "(inputfiles=\n";
    for (GroupedJob job : *this)
        for(ARex::FileData file : job.jobRef->GetLocalDescription()->inputdata)
            desc_str += BuildFileDesc(file);
    
    for (auto entry : fs::directory_iterator(inputPath)){
        if (entry.is_regular_file()){
            fs::path input = entry.path();

            if (! input.is_absolute())
                input = fs::absolute(input);
            
            desc_str += "(\"" + input.generic_string() + "\" \"" + input.filename().generic_string() + "\")\n";
        }
    }

    desc_str += "(\"groupConfig\" \"" + fs::path(basePath).append("groupConfig").generic_string() + "\")\n";
    desc_str += "(\"GROUPED\" \"/arc/GROUPED\"))\n";
        
    // Outputs
    logger.msg(Arc::DEBUG, "> Building output files");
    desc_str += "(outputfiles=\n";

    for (GroupedJob job : *this) {
        for(ARex::FileData file : job.jobRef->GetLocalDescription()->outputdata)
            desc_str += BuildFileDesc(file);

        for(std::string output : job.outputs)
            desc_str += "(\"/" + job.jobRef->get_id() + output + ")";
        
    }
    
    desc_str += "(\"/\" \"\" ))\n";
    
    // RTEs
    // TODO: Check for duplicate RTEs
    logger.msg(Arc::DEBUG, "> Building RTEs");
    if(false){
        desc_str += "(runtimeEnvironment=\n";
        for (GroupedJob job : *this) {
            desc_str+="\"output.txt\"\n";
        }
        desc_str += ")\n";
    }

    // Environ
    // TODO: Check for duplicate Envirnment variables
    logger.msg(Arc::DEBUG, "> Building environment");
    if (false){
        desc_str += "(environment=\n";
        for (GroupedJob job : *this) {
            desc_str+="(\"ENV_VAR\" \"VAR\" )\n";
        }
        desc_str += ")";
    }

    std::string deleg_id = GetDelegationId(config, senderPEMLocation, senderDn);  
    if (deleg_id == "") {
        return;
    }

    // Rest
    logger.msg(Arc::DEBUG, "> Building final info");
    desc_str += 
        // "(executables=workloadExec.sh)\n" // ?????
        "(queue=" + front().jobRef->GetLocalDescription()->queue + ")\n"
        "(delegationid=" + deleg_id + ")\n";

    std::string xlrsPath("/tmp/group_" + getId() + ".xlrs");

    logger.msg(Arc::DEBUG, "Saving group XLRS at %s", xlrsPath);

    std::ofstream stream(xlrsPath);
    stream << desc_str;
    stream.close();

    __pid_t pid = fork();

    logger.msg(Arc::DEBUG, "Launching execlp");
    if (pid < 0)
        throw std::exception();

    if (pid == 0){
        execlp(
            "arcsub", 
            "arcsub",
            "-d", 
            "DEBUG", 
            "-C", 
            "$HOSTNAME", 
            "-T", 
            "arcrest", 
            "-Q", 
            "None", 
            xlrsPath.c_str(), 
            NULL
        );
        logger.msg(Arc::DEBUG, "CHILD DONE");
        return;
    }
    
    logger.msg(Arc::DEBUG, "PARENT DONE");
};

JobGroup::GroupState JobGroup::addToGroup(ARex::GMJobRef &jobRef, const ARex::GMConfig& config){
    // Check if it passes the check of this job group
    if (! checker.isValid(*jobRef->GetLocalDescription())){
        logger.msg(Arc::DEBUG, "Group '%s' failed JLD check", name);
        return JobGroup::GroupState::FAILED;
    }

    // Create a GroupedJob
    bool storeJob = true;
    GroupedJob* job;

    // Check if it is already in the group
    for (GroupedJob& _job : *this){
        if (_job.jobRef->get_id() == jobRef->get_id()){ // Already in group
            logger.msg(Arc::WARNING, "Job '%s' already in group '%s'", _job.jobRef->get_id(), name);
            storeJob = false;
            job = &_job;
            break;
        }
    }

    // Store the Job
    if(storeJob){
        std::string jobCtrl = ARex::job_control_path(config.ControlDir(), jobRef->get_id(), NULL);
        job = new GroupedJob(jobRef, jobCtrl);
        logger.msg(Arc::INFO, "Adding job '%s' to group '%s'", job->jobRef->get_id(), name);
        this->push_back(*job);
    }

    // Check if their inputs are ready to be moved
    if (job->AreInputsReady(config))
        job->MoveInputs(inputPath.generic_string());

    // Check last timestamp
    std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();

    if (firstJobTime == std::chrono::steady_clock::time_point{}){
        firstJobTime = now;
        return JobGroup::GroupState::STORED;
    }

    // Check if it can be sent
    logger.msg(Arc::DEBUG, "Checking callback");
    long elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - firstJobTime).count();

    if (this->size() >= max_jobs || elapsed > max_seconds){
        // Check every job, if any hasn't moved their input, don't send
        for (GroupedJob _job : *this)
            if (!_job.inputsMoved)
                return JobGroup::GroupState::STORED;
        
        return JobGroup::GroupState::READY;
    }

    return JobGroup::GroupState::STORED;
};

//
// JobGroups
//
void JobGroups::buildGroups(const ARex::GMConfig& config){
    // Only build once
    if(!groups.empty())
        return;

    // Initialize the group directory
    int res = mkdir("/tmp/groups", S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    int err = errno;
    
    if (res != 0 && err != EEXIST){
        logger.msg(Arc::FATAL, "Error while creating directory '/tmp/groups': %s", err);
        throw std::exception();
    }

    // Build the groups
    for(std::string name : config.JobGroupNames()){
        logger.msg(Arc::DEBUG, "No groups found with name '%s', creating one", name);
        groups.insert(std::pair(
            name, 
            new JobGroup(
                name, 
                *config.JobGroups(name)
            )
        ));
    }
};

bool JobGroups::tryAddJob(ARex::JobsList* jobList, ARex::GMJobRef& jobRef, const ARex::GMConfig& config){
    // Build the groups if not done already
    buildGroups(config);

    std::string dirStrPath = jobRef->SessionDir();
    const std::filesystem::path dirPath(dirStrPath);
    fs::directory_iterator dirIter = fs::directory_iterator(fs::directory_entry(dirPath));
    
    // Check if it is a group job
    logger.msg(Arc::VERBOSE, "Looking for GROUPED file in %s", dirStrPath);
    for (fs::directory_entry entry : dirIter) {
        if (entry.is_regular_file() && entry.path().filename() == "GROUPED"){
            logger.msg(Arc::VERBOSE, "The job is a group, won't try to add it");
            return false;
        }
    }

    // Try to add the job onto a group
    for (std::map<std::string, std::unique_ptr<JobGroup>>::iterator iter = groups.begin(); iter != groups.end(); ++iter){
        JobGroup::GroupState status = iter->second->addToGroup(jobRef, config);

        if (status == JobGroup::GroupState::FAILED){
            logger.msg(Arc::DEBUG, "Job group not stored at %s", iter->first);
            continue; // Try on another group
        }
        
        if (status == JobGroup::GroupState::READY){
            logger.msg(Arc::INFO, "Job group '%s' ready to be sent", iter->first);
            // iter->second->SendGroup(jobList, config);
            iter->second->SendExec(config);
        }
        else if (status == JobGroup::GroupState::STORED){
            logger.msg(Arc::DEBUG, "The job stored in a group given by '%s'", iter->first);
        }

        return true;    
    }

    logger.msg(Arc::VERBOSE, "No valid groups to store the job at");
    return false;
};

}

//
// GMConfig
//
const IFIC::GroupConfig* ARex::GMConfig::JobGroups(std::string groupName) const {
  std::map<std::string, IFIC::GroupConfig>::const_iterator iter = job_groups.find(groupName);
  
  if (iter == job_groups.end()){
    logger.msg(Arc::WARNING, "Unable to find group '%s'", groupName);
    return nullptr;
  }

  return &(iter->second);
}

const std::list<std::string> ARex::GMConfig::JobGroupNames() const {
    std::list<std::string> names = std::list<std::string>();

    for (std::pair<const std::string, IFIC::GroupConfig> pair : job_groups){
        names.push_front(pair.first);
    }

    return names;
}

