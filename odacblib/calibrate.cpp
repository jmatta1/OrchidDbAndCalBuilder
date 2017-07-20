#include<TFile.h>
#include<TH1.h>
#include<TH2.h>
#include<sstream>
using std::ostringstream;

extern "C" void performCalSum(int, int, int, int, char*, char*, int);

void performCalSum(int startRun, int stopRun, int detNum, int calNum, char* inRootName, char* outRootName, int newFile)
{
    TFile* inFile = new TFile(inRootName);
    TFile* outFile = nullptr;
    if(newFile==0)
    {
        outFile = new TFile(outRootName, "RECREATE");
    }
    else
    {
        outFile = new TFile(outRootName, "UPDATE");
    }
    
    //make an ostringstream to name objects
    ostringstream namer;
    //First open the first run and clone them to make the sum files
    //First the X-projection
    namer << "Det_"<<detNum<<"_Run_"<<startRun<<"_px";
    TH1D* temp = (TH1D*)inFile->Get(namer.str().c_str());
    namer.str("");
    namer.clear();
    namer << "Det_"<<detNum<<"_Sum_px_Cal_"<<calNum;
    outFile->cd();
    TH1D* sumPx = (TH1D*)temp->Clone();
    sumPx->SetName(namer.str().c_str());
    delete temp;
    
    //Then the X-projection with threshold
    namer.str("");
    namer.clear();
    namer << "Det_"<<detNum<<"_Run_"<<startRun<<"_px_thresh";
    temp = (TH1D*)inFile->Get(namer.str().c_str());
    namer.str("");
    namer.clear();
    namer << "Det_"<<detNum<<"_Sum_px_thresh_Cal_"<<calNum;
    outFile->cd();
    TH1D* sumPxThresh = (TH1D*)temp->Clone();
    sumPxThresh->SetName(namer.str().c_str());
    delete temp;
    
    //First the Y-projection
    namer.str("");
    namer.clear();
    namer << "Det_"<<detNum<<"_Run_"<<startRun<<"_py";
    temp = (TH1D*)inFile->Get(namer.str().c_str());
    namer.str("");
    namer.clear();
    namer << "Det_"<<detNum<<"_Sum_py_Cal_"<<calNum;
    outFile->cd();
    TH1D* sumPy = (TH1D*)temp->Clone();
    sumPy->SetName(namer.str().c_str());
    delete temp;
    
    //Then the Y-projection with threshold
    namer.str("");
    namer.clear();
    namer << "Det_"<<detNum<<"_Run_"<<startRun<<"_py_thresh";
    temp = (TH1D*)inFile->Get(namer.str().c_str());
    namer.str("");
    namer.clear();
    namer << "Det_"<<detNum<<"_Sum_py_thresh_Cal_"<<calNum;
    outFile->cd();
    TH1D* sumPyThresh = (TH1D*)temp->Clone();
    sumPyThresh->SetName(namer.str().c_str());
    delete temp;
    
    //Then the 2D histogram
    namer.str("");
    namer.clear();
    namer << "Det_"<<detNum<<"_Run_"<<startRun<<"_2D";
    TH2F* temp2 = (TH2F*)inFile->Get(namer.str().c_str());
    namer.str("");
    namer.clear();
    namer << "Det_"<<detNum<<"_Sum_2D_Cal_"<<calNum;
    outFile->cd();
    TH2F* sum2D = (TH2F*)temp2->Clone();
    sum2D->SetName(namer.str().c_str());
    delete temp2;

    for(int i=(startRun+1); i<=stopRun; ++i)
    {
        namer.str("");
        namer.clear();
        namer << "Det_"<<detNum<<"_Run_"<<i<<"_2D";
        TH2F* temp2D = (TH2F*)inFile->Get(namer.str().c_str());
        sum2D->Add(temp2D);
        delete temp2D;
        namer.str("");
        namer.clear();
        namer <<"Det_"<<detNum<<"_Run_"<<i<<"_px";
        TH1D* temp1D = (TH1D*)inFile->Get(namer.str().c_str());
        sumPx->Add(temp1D);
        delete temp1D;
        namer.str("");
        namer.clear();
        namer <<"Det_"<<detNum<<"_Run_"<<i<<"_px_thresh";
        temp1D = (TH1D*)inFile->Get(namer.str().c_str());
        sumPxThresh->Add(temp1D);
        delete temp1D;
        namer.str("");
        namer.clear();
        namer <<"Det_"<<detNum<<"_Run_"<<i<<"_py";
        temp1D = (TH1D*)inFile->Get(namer.str().c_str());
        sumPy->Add(temp1D);
        delete temp1D;
        namer.str("");
        namer.clear();
        namer <<"Det_"<<detNum<<"_Run_"<<i<<"_py_thresh";
        temp1D = (TH1D*)inFile->Get(namer.str().c_str());
        sumPyThresh->Add(temp1D);
        delete temp1D;
    }
    outFile->cd();
    sum2D->Write();
    sumPx->Write();
    sumPy->Write();
    sumPxThresh->Write();
    sumPyThresh->Write();
    outFile->Flush();
    delete sum2D;
    delete sumPx;
    delete sumPy;
    delete sumPxThresh;
    delete sumPyThresh;
    delete outFile;
    delete inFile;
}
