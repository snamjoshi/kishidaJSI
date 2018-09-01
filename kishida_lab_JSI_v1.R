###############################################################################
### Functions for cluster script, version 1                                 ###
###############################################################################
### Author: Sanjeev Namjoshi                                                ###
### Email: snamjosh@wakehealth.edu                                          ###
###############################################################################
# Load packages -------------------------------------------------------------------------------
library(tools)
library(magrittr)

# Intro messages ----------------------------------------------------------
message("====================================================================")
message("Kishida Lab Job Submission Interface\nVersion: 1.0\nAuthor: Sanjeev Namjoshi\nContact: snamjosh@wakehealth.edu")
message("====================================================================")
message("Please ensure the following requirements are met before proceeding:\n 1. You sourced this file while in the root folder of the IRB directory.\n 2. Your modules are loaded.\n 3. Your data folders are in /original/project/worker/.")
message(" ")
message("Instructions:\n * Use setup() to create a new worker folder and to_archive folder or to create new prefs.txt and dirs.txt files.\n * Use runAnalysis() to create a SLURM script and submit a job based on the content of your R script, dirs.txt, and prefs.txt.\n * Use archive() to archive anything in the to_archive folder.\n * Use prepareFilesForAnalysis() followed by submitToSlurm() if you wish to modify the SLURM script before submitting for analysis.\n * Use resubmitJob() to resubmit a job with the current scripts and preferences. Note that this may overwrite any data in scratch/project/worker/job.\n * Use checkPackages() to check packages that are installed on the system.\n * Use moveOutputFilesToJobFolders() to move SLURM output into /scratch.")
message(" ")
message("Please submit an issue at https://github.com/snamjoshi/kishidaJSI/issues for comments, questions, or feature requests.")

# Functions ---------------------------------------------------------------
### Creates a new worker directory with project subdirectory. 
#> If worker already exists, just creates a project directory.
setup <- function(workerName, projectName) {
  ### Check to make sure project folder exists in all directories
  if(projectName %in% list.files("original") == FALSE) stop(paste("Project ", projectName, " not found in /original.", sep = ""))
  if(projectName %in% list.files("final") == FALSE) stop(paste("Project ", projectName, " not found in /original.", sep = ""))
  if(projectName %in% list.files("scratch") == FALSE) stop(paste("Project ", projectName, " not found in /original.", sep = ""))
  
  ### Check if worker folder exists in project folders for scratch/final. If not, create it.
  if(workerName %in% list.files(paste("scratch", projectName, sep = "/")) == FALSE) {
    dir.create(paste("scratch", projectName, workerName, sep = "/"))
    message(paste("Directory created for worker ", workerName, " in /scratch.", sep = ""))
  }
  
  if(workerName %in% list.files(paste("final", projectName, sep = "/")) == FALSE) {
    dir.create(paste("final", projectName, workerName, sep = "/"))
    message(paste("Directory created for worker ", workerName, " in /final.", sep = ""))
  }
  
  ### Check if to_archive exists in scratch project folder. If not, create it.
  if("to_archive" %in% list.files(paste("scratch", projectName, workerName, sep = "/")) == FALSE) {
    dir.create(paste("scratch", projectName, workerName, "to_archive", sep = "/"))
  }
  
  projectDir <- paste("scratch/", workerName, "/", projectName, sep = "")
  
  ### Create template prefs file for user
  prefs <- c("--cpus-per-task=", "--job-name=", "--mem-per-cpu=", "--nodes=", "--ntasks=", "--ntasks-per-core=", "--ntasks-per-node=", "--time=")
  
  #> Check if prefs.txt or dirs.txt already exists in project folder. If not, create them.
  if(file.exists(paste("scratch", projectName, workerName, "prefs.txt", sep = "/")) == FALSE) {
    writeLines(prefs, paste("scratch", projectName, workerName, "prefs.txt", sep = "/"))
  }
  
  if(file.exists(paste("scratch", projectName, workerName, "dirs.txt", sep = "/")) == FALSE) {
    writeLines("", paste("scratch", projectName, workerName, "dirs.txt", sep = "/"))
  }
  
  message("Setup complete.")
  message("Please put your R/MATLAB script in scratch/project/worker before proceeding.")
}

### Run analysis is a wrapper around prepareFilesForAnalysis() and submitToSlurm()
runAnalysis <- function(workerName, projectName, jobName, scriptName, scriptType, email = NULL, mpthreads = 1) {
  ### Check for missing parameters
  allParam <- ls()
  passedParam <- names(as.list(match.call())[-1])
  
  if (any(!allParam %in% passedParam)) {
    stop(paste("Please input a value for", paste(setdiff(allParam, passedParam), collapse = ", ")))
  }
  
  prepareFilesForAnalysis(workerName, projectName, jobName, scriptName, scriptType, email, mpthreads)
  submitToSlurm(workerName, projectName, jobName, scriptName)
}

prepareFilesForAnalysis <- function(workerName, projectName, jobName, scriptName, scriptType, email = NULL, mpthreads = 1) {
  ### Check for missing parameters
  allParam <- ls()
  passedParam <- names(as.list(match.call())[-1])
  
  if (any(!allParam %in% passedParam)) {
    stop(paste("Please input a value for", paste(setdiff(allParam, passedParam), collapse = ", ")))
  }
  
  ### Load relevant variables
  projectDir <- paste("scratch", projectName, workerName, sep = "/")
  prefTable <- read.table(paste(projectDir, "/", "prefs.txt", sep = ""), sep = "=", col.names = c("prefVar", "value"), na.strings = "", stringsAsFactors = FALSE)
  dirsVec <- suppressWarnings(readLines(paste(projectDir, "/", "dirs.txt", sep = "")))
  dirsVec <- dirsVec[dirsVec != ""]   # Trim whitespace
  dataFolders <- sapply(strsplit(dirsVec, "/"), tail, 1)
  
  message("Checking input files...")
  
  checkWorkerInput(workerName, projectName, jobName, scriptName, scriptType, projectDir, prefTable, dirsVec)
  
  message("Ready to build SLURM scripts.")
  
  # Confirmation dialog
  chosen = FALSE
  while(chosen == FALSE) {
    confirmation <- readline("Are you sure you want to proceed? (y/n): ")
    
    if(toupper(confirmation) == "Y") {
      message("Creating SLURM script and building job folders...")
      chosen = TRUE
    }
    
    if(toupper(confirmation) == "N") {
      stop("Job preparation aborted by worker.")
    }
    
    if(!toupper(confirmation) %in% c("Y", "N")) {
      message("Unexpected input. Please enter either (y) or (n).")
    }
  }

  buildSlurmScripts(prefTable, mpthreads, email, dirsVec, scriptName, projectDir, jobName, scriptType)
  createJobsFolders(projectDir, dataFolders, jobName)
  
  ### This function is necessary to ensure that the output files from SLURM can be matched
  ### to the job folders in /scratch/project/worker/job
  takeSnapshot(projectDir, jobName, dirsVec)
}

checkWorkerInput <- function(workerName, projectName, jobName, scriptName, scriptType, projectDir, prefTable, dirsVec) {
  ### Check for worker
  if(workerName %in% list.files(paste("scratch", projectName, sep = "/")) == FALSE) stop(paste("Worker ", workerName, " does not exist in /scratch. Please run the new worker setup script.", sep = ""))
  if(workerName %in% list.files(paste("original", projectName, sep = "/")) == FALSE) stop(paste("Worker ", workerName, " does not exist in /original. Please run the new worker setup script.", sep = ""))
  if(workerName %in% list.files(paste("final", projectName, sep = "/")) == FALSE) stop(paste("Worker ", workerName, " does not exist in /final. Please run the new worker setup script.", sep = ""))
  
  ### Check for project
  if(projectName %in% list.files("scratch") == FALSE) stop(paste("Project ", projectName, " not found in /scratch.", sep = ""))
  if(projectName %in% list.files("original") == FALSE) stop(paste("Project ", projectName, " not found in /original.", sep = ""))
  if(projectName %in% list.files("final") == FALSE) stop(paste("Project ", projectName, " not found in /final", sep = ""))
  
  ### Check for scripts and script type
  if(file.exists(paste(projectDir, "/", scriptName, sep = "")) == FALSE) stop("Cannot find script file.")
  
  if(toupper(scriptType) == "R") {
    if(file_ext(toupper(scriptName)) != "R") stop("Worker has not submitted and R script.")
    if((toupper(scriptType) == file_ext(scriptName)) == FALSE) stop("scriptName and scriptType do not match.")
  }
  
  if(toupper(scriptType) == "MATLAB") {
    if(file_ext(toupper(scriptName)) != "M") stop("Worker has not submitted a Matlab script.")
    if((toupper(file_ext(scriptName)) == "M") == FALSE) stop("scriptName and scriptType do not match.")
  }
  
  if(!(toupper(scriptType) %in% c("R", "MATLAB"))) {
    stop("Script format not recognized. Please submit R or Matlab scripts only.")
  }
  
  ### Check if text files exist
  if("prefs.txt" %in% list.files(projectDir) == FALSE) stop("Cannot find prefs.txt file.")
  if("dirs.txt" %in% list.files(projectDir) == FALSE) stop("Cannot find dirs.txt file.")
  
  ### Check job name
  if(grepl("^[a-zA-Z0-9_]*$", jobName) == FALSE) stop("Job name can only contain letters, numbers, and underscores.")
  if(jobName %in% list.files(projectDir) == TRUE) stop("Job already exists.")
  
  ### Check content of dirs and prefs files
  if(identical(prefTable$prefVar, c("--cpus-per-task", "--job-name", "--mem-per-cpu", "--nodes", "--ntasks", "--ntasks-per-core", "--ntasks-per-node", "--time")) == FALSE) stop("Parameters in prefs file missing or incorrect.")
  if(all(is.na(prefTable$value)) == TRUE) stop("Please fill in your prefs.txt file.")
  if(all(dir.exists(paste(getwd(), dirsVec, sep = ""))) == FALSE) stop("Some or all directories not found in /original.")
  if((prefTable[prefTable$prefVar == "--job-name", "value"] == jobName) == FALSE) stop("Job name in prefs.txt does not match jobName parameter.")
}

### Used to automate the creation of the SLURM script section for the R script that points
### to the directory where the data is and the directory where the R script to run over
### the data directories is.
createJobCodeR <- function(dirsVec, scriptName, projectDir) {
  parDir <- getwd()
  jobCode = NULL
  for(i in 1:length(dirsVec)) {
    nextCode <- c(paste("cd ", parDir, dirsVec[i], sep = ""),
                  paste("Rscript", paste(parDir, projectDir, scriptName, sep = "/")),
                  "cd -",
                  "")
    jobCode <- c(jobCode, nextCode)
  }
  
  return(jobCode)
}

### Used to automate the creation of the SLURM script section for the Matlab script that points
### to the directory where the data is and the directory where the Matlab script to run over
### the data directories is.
createJobCodeMatlab <- function(dirsVec, scriptName, projectDir) {
  parDir <- getwd()
  jobCode = NULL
  for(i in 1:length(dirsVec)) {
    nextCode <- c(paste("cd ", parDir, dirsVec[i], sep = ""),
                  paste("matlab -nodesktop -r ", "\'", paste(parDir, "/", projectDir, "/", scriptName, ", quit", "\'", sep = ""), sep = ""),
                  "cd -",
                  "")
    jobCode <- c(jobCode, nextCode)
  }
  
  return(jobCode)
}

### Based on the user input and file locations, this function generates the SLURM shell
### script for execution.
buildSlurmScripts <- function(prefTable, mpthreads, email, dirsVec, scriptName, projectDir, jobName, scriptType) {
  prefTableFiltered <- prefTable[complete.cases(prefTable), ]

  if(toupper(scriptType) == "R") {
    script <- c("#!/bin/bash",
                paste("#SBATCH ", paste(prefTableFiltered$prefVar, prefTableFiltered$value, sep = "="), sep = ""),
                paste("#SBATCH --output=", paste(projectDir, jobName, "", sep = "/"), jobName, "_%J.out", sep = ""),
                paste("#SBATCH --error=", paste(projectDir, jobName, "", sep = "/"), jobName, "_%J.err", sep = ""),
                paste("#SBATCH --mail-type=END"),
                if(is.character(email) == TRUE) paste("#SBATCH --mail-user=", email, sep = ""),
                "",
                "set -x",
                "",
                paste("export OMP_NUM_THREADS=", mpthreads, sep = ""),
                "",
                createJobCodeR(dirsVec, scriptName, projectDir),
                "wait",
                "exit")
    
    writeLines(script, paste(projectDir, "/", jobName, ".sh", sep = ""))
  }
  
  if(toupper(scriptType) == "MATLAB") {
    script <- c("#!/bin/bash",
                paste("#SBATCH ", paste(prefTableFiltered$prefVar, prefTableFiltered$value, sep = "="), sep = ""),
                paste("#SBATCH --output=", paste(projectDir, jobName, "", sep = "/"), jobName, "_%J.out", sep = ""),
                paste("#SBATCH --error=", paste(projectDir, jobName, "", sep = "/"), jobName, "_%J.err", sep = ""),
                paste("#SBATCH --mail-type=END"),
                if(is.character(email) == TRUE) paste("#SBATCH --mail-user=", email, sep = ""),
                "",
                "set -x",
                "",
                paste("export OMP_NUM_THREADS=", mpthreads, sep = ""),
                "",
                createJobCodeMatlab(dirsVec, scriptName, projectDir),
                "wait",
                "exit")
    
    writeLines(script, paste(projectDir, "/", jobName, ".sh", sep = ""))
  }
}

### After the user has confirmed they want to proceed, a job folder is created with 
### subdirectories that match the name of the data folders the script is to be run over.
createJobsFolders <- function(projectDir, dataFolders, jobName) {
  jobPath <- paste(projectDir, "/", jobName, sep = "")
  dirPaths <- paste(jobPath, "/", dataFolders, sep = "")
  
  if(dir.exists(jobPath) == TRUE) stop("A directory with that job name already exists")
  dir.create(jobPath)
  sapply(1:(length(dirPaths)), function(x) dir.create(dirPaths[x]))
}

submitToSlurm <- function(workerName, projectName, jobName, scriptName) {
  ### Check for missing parameters
  allParam <- ls()
  passedParam <- names(as.list(match.call())[-1])
  
  if (any(!allParam %in% passedParam)) {
    stop(paste("Please input a value for", paste(setdiff(allParam, passedParam), collapse = ", ")))
  }
  
  projectDir <- paste("scratch", projectName, workerName, sep = "/")
  dirsVec <- suppressWarnings(readLines(paste(projectDir, "/", "dirs.txt", sep = "")))
  dirsVec <- dirsVec[dirsVec != ""]   # Trim emptylines
  parDir <- getwd()
  
  ### Copy dirs.txt, prefs.txt, the R script, and the SLURM script into the job directory
  copySetupFilestoJobFolder("dirs.txt", jobName, projectDir)
  copySetupFilestoJobFolder("prefs.txt", jobName, projectDir)
  copySetupFilestoJobFolder(scriptName, jobName, projectDir)
  copySetupFilestoJobFolder(paste(jobName, ".sh", sep = ""), jobName, projectDir)
  
  # Move into input directory and run SLURM script
  system(paste("sbatch", paste(projectDir, paste(jobName, ".sh", sep = ""), sep = "/")))
  message(paste(jobName, "submitted to SLURM queue."), sep = " ")
  message("Enter:\n squeue -u [your_user_name]\nin the terminal to see the status of your running jobs.")
}

### Used to automate the file moving process in submitToSlurm()
copySetupFilestoJobFolder <- function(fileName, jobName, projectDir) {
  file.copy(from = paste(projectDir, fileName, sep = "/"), 
              to = paste(projectDir, jobName, fileName, sep = "/"))
}

### Used in situations where a job fails or is not run properly and needs to be rerun with
### the exact same settings.
resubmitJob <- function(workerName, projectName, jobName, scriptName, exclude = NULL) {
  ### Check for missing parameters
  allParam <- ls()
  passedParam <- names(as.list(match.call())[-1])
  
  if (any(!allParam %in% passedParam)) {
    stop(paste("Please input a value for", paste(setdiff(allParam, passedParam), collapse = ", ")))
  }
  
  ### Check to make sure the exclude parameter is in a valid format
  if((is.character(exclude) || is.null(exclude)) == FALSE) stop("Exclude parameter must be a character vector.")
  
  if(is.character(exclude) == TRUE) {
    if(unique(nchar(exclude)) != 3) stop("Excluded nodes must be of the format XXX, e.g. 005, 021, 125 etc.")
  }
  
  ### Confirmation dialog
  message("WARNING!!! This function is intended to be used only if a job fails/ends prematurely and you would like to rerun it with the exact same settings or rerun by excluding certain nodes. If the resubmitted job is successful it WILL overwrite anything in scratch/project/worker/job.")
  message(" ")
  chosen = FALSE
  while(chosen == FALSE) {
    message(paste("You are about to rerun", scriptName, "over the", jobName, "folder with settings found in dirs.txt/prefs.txt."))
    confirmation <- readline("Are you sure you want to proceed? (y/n): ")
    
    if(toupper(confirmation) == "Y") {
      chosen = TRUE
    }
    
    if(toupper(confirmation) == "N") {
      stop("Job resubmission aborted by worker.")
    }
    
    if(!toupper(confirmation) %in% c("Y", "N")) {
      message("Unexpected input. Please enter either (y) or (n).")
    }
  }
  
  ### Resubmit to SLURM
  if(is.character(exclude) == TRUE) system(paste("sbatch ", paste(projectDir, paste(jobName, ".sh", sep = ""), sep = "/"), " --exclude=demon[", paste(exclude, collapse = ","), "]", sep = ""))
  message(paste(jobName, "resubmitted to SLURM queue."), sep = " ")
  message("Enter:\n squeue -u [your_user_name]\nin the terminal to see the status of your running jobs.")
}

### Simple function to move the files in to_archive into the /final directory
archive <- function(workerName, projectName) {
  to_archive <- list.dirs(paste("scratch", projectName, workerName, "to_archive", sep = "/"), recursive = FALSE)
  file.copy(to_archive, paste("final", projectName, workerName, sep = "/"), recursive = TRUE)
  
  message("Files successfully archived.")
}

### A simple function to determine which packages are installed on the system based on an
### input character vector.
checkPackages <- function(pkg) {
  setNames(pkg %in% installed.packages()[, "Package"], pkg)
}

### Unfortunately, with the current setup there is no way to actually ensure that the output
### of the R script run over a specific directory is output anywhere other than the directory
### it is running over. The takeSnapshot() and moveOutputFilesToJobFolders() functions try
### to overcome this.
takeSnapshot <- function(projectDir, jobName, dirsVec) {
  dir.create(paste(projectDir, jobName, "snapshots", sep = "/"))
  
  ### Export snapshot of all dirs to working directory
  ### This records what files are currently in the directory.
  dirSnapshot <- NULL
  for(i in 1:length(dirsVec)) {
    oldSnapshot <- fileSnapshot(path = paste(getwd(), dirsVec[i], sep = "/"))
    saveRDS(oldSnapshot, paste(paste(projectDir, jobName, "snapshots/", sep = "/"), "old_snapshot_", i, ".rds", sep = ""))
  }
}

### After the job is run, this function looks in the data directories where SLURM has output
### the data files from R/Matlab. It compares when is now in there to what was in there
### prior to the run. Any new files are presumed to be created from the SLURM run and are
### moved to the job folder.
moveOutputFilesToJobFolders <- function(workerName, projectName, jobName) {
  projectDir <- paste("scratch", projectName, workerName, sep = "/")
  
  dirsVec <- suppressWarnings(readLines(paste(projectDir, jobName, "dirs.txt", sep = "/")))
  dirsVec <- dirsVec[dirsVec != ""]   # Trim whitespace
  
  # Take a new snapshot
  newSnapshot <- NULL
  for(i in 1:length(dirsVec)) {
    newSnapshot <- fileSnapshot(path = paste(getwd(), dirsVec[i], sep = "/"))
    saveRDS(newSnapshot, paste(paste(projectDir, jobName, "snapshots/", sep = "/"), "new_snapshot_", i, ".rds", sep = ""))
  }
  
  # Compare new snapshot to one taken before SLURM script was submitted
  added <- list()
  for(i in 1:length(dirsVec)) {
    preInfo <- readRDS(list.files(paste(projectDir, jobName, "snapshots", sep = "/"), "old", full.names = TRUE)[i])
    postInfo <- readRDS(list.files(paste(projectDir, jobName, "snapshots", sep = "/"), "new", full.names = TRUE)[i])
    added[[i]] <- paste(postInfo$path, "/", setdiff(rownames(postInfo$info), rownames(preInfo$info)), sep = "")
  } 
  
  added <- unlist(added)
  
  # Move files based on what was newly added.
  file.rename(from = added,
              to = paste(getwd(), projectDir, jobName, sapply(strsplit(added, "/"), function(x) x[length(x)-1]), sapply(strsplit(added, "/"), tail, 1), sep = "/"))
  
  # Remove snapshots folder 
  unlink(paste(projectDir, jobName, "snapshots", sep = "/"), recursive = TRUE)
  
  message("SLURM output moved to job folders in /scratch.")
}