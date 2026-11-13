package com.hysd.hyscreen;

import android.os.Binder;
import android.os.IBinder;
import android.os.IInterface;
import android.os.RemoteException;

public interface HYSDInterface extends IInterface {
    void basicTypes(int anInt, long aLong, boolean aBoolean, float aFloat, double aDouble, String aString) throws RemoteException;
    void finishkey(int calibState, int keyState, int errorCode) throws RemoteException;
    float[] getangles() throws RemoteException;
    int getcast() throws RemoteException;
    int[] getkey() throws RemoteException;
    float[] getsensordata() throws RemoteException;
    float[] gettofdata() throws RemoteException;
    int gettofstate() throws RemoteException;
    int init() throws RemoteException;
    int motorb() throws RemoteException;
    int motorf() throws RemoteException;
    int motorg() throws RemoteException;
    float motorg2() throws RemoteException;
    int motors() throws RemoteException;
    int setcast(int mode) throws RemoteException;
    int setkey(int ltx, int lty, int lbx, int lby, int rbx, int rby, int rtx, int rty) throws RemoteException;
    int unint() throws RemoteException;
    int updateui(int state) throws RemoteException;

    abstract class Stub extends Binder implements HYSDInterface {
        public static HYSDInterface asInterface(IBinder obj) { return null; }
    }
}
