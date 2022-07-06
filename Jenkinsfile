def getCommitSha() {
  sh "git show-ref -s $GIT_BRANCH > .git/current-commit"
  return readFile(".git/current-commit").trim()
}

def getRepoURL() {
  sh "git config --get remote.origin.url > .git/remote-url"
  return readFile(".git/remote-url").trim()
}

def updateGithubCommitStatus() {
  repoUrl = getRepoURL()
  commitSha = getCommitSha()

  step([
    $class: 'GitHubCommitStatusSetter',
    reposSource: [$class: "ManuallyEnteredRepositorySource", url: repoUrl],
    commitShaSource: [$class: "ManuallyEnteredShaSource", sha: commitSha],
    errorHandlers: [[$class: 'ShallowAnyErrorHandler']],
  ])
}


pipeline {
    agent any
    environment {
        NEXUS_COMMON_CREDS = credentials('nexus_cred')
        NEXUS_COMMON_CREDS_USR = credentials('nexus_cred')
        NEXUS_COMMON_CREDS_PSW = credentials('nexus_cred')
    }
    stages {
        stage('Test') {
            steps {
                sh '''
                  make test '''
            }
        }
        stage('Build') {
            steps {
                sh '''
          make publish '''
            }
        }
        stage('PyPi Index Delivery') {
            when {
                branch 'master'
            }
            steps {
                script {
                    env.FILE_NAME = sh(script: 'ls dist/*gz', returnStdout: true).trim()
                    sh '''
                    python3 -m twine upload -u $NEXUS_COMMON_CREDS_USR -p $NEXUS_COMMON_CREDS_PSW  --repository-url $NEXUS_PIP dist/* '''
                    if (manager.logContains('.*HTTPError*')) {
                        error("Build failed. See logs")
                    }

                }
            }
        }
        stage('Clean Distribution Files') {
            steps {
                sh '''
          make clean'''
            }
        }
        stage('Pack') {
            steps {
                sh '''
          make pack '''
            }
        }
        stage('Storage Delivery') {
            steps {
                script {
                    env.FILE_NAME = sh(script: 'ls *gz', returnStdout: true).trim()
                    env.NAME_WEXT = sh(script: "ls *gz | sed -e 's/.tar.gz\$//'", returnStdout: true).trim()
                    env.NAME_BEFORE = sh(script: "ls *gz | sed -e 's/-[0-9].*//'", returnStdout: true).trim()
                    env.BUILD_NUMBER = sh(script: 'printf "%04d" $BUILD_NUMBER', returnStdout: true).trim()
                    sh '''
                  curl -v -u $NEXUS_COMMON_CREDS --upload-file $FILE_NAME $NEXUS_REPO_URL/repository/components/$NAME_BEFORE/$NAME_WEXT-$BUILD_NUMBER.tar.gz'''
                    if (!manager.logContains('.*We are completely uploaded and fine.*')) {
                        error("Build failed. See logs")
                    }
                }
            }
        }
    }
    post {
        always {
    updateGithubCommitStatus()
        }
        success {
            sh '''echo $FILE_NAME successfully downloaded'''
        }
    }
}