pwd
ls
git init
git remote -v
git remote add git@rwes-gitlab01.internal.imsglobal.com:PredA/bdf_dev.git
git remote add origin git@rwes-gitlab01.internal.imsglobal.com:PredA/bdf_dev.git
git remote -v
git status
nano .gitignore 
git add .
git commit -am "initial commit from the CDSW platform"
git push origin master
git remote remove origin
git remote -v
git remote add origin http://rwes-gitlab01.internal.imsglobal.com/PredA/bdf_dev.git
git push origin master
pip3 install networkx
python3
