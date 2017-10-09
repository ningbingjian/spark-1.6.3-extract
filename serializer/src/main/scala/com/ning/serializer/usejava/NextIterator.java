package com.ning.serializer.usejava;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Created by zhaoshufen
 * User:  zhaoshufen
 * Date: 2017/9/29
 * Time: 22:04
 * To change this setting on:Preferences->editor->File and Code Templates->Include->File Header
 */
public abstract class NextIterator<T> implements Iterator<T> {
    private boolean gotNext = false ;
    private T nextValue = null ;
    private boolean closed = false ;
    protected boolean finished = false ;


    public abstract T getNext();
    protected abstract void close();
    public void closeIfNeeded() {
        if (!closed) {
            // Note: it's important that we set closed = true before calling close(), since setting it
            // afterwards would permit us to call close() multiple times if close() threw an exception.
            closed = true ;
            close();
        }
    }
    @Override
    public boolean hasNext() {
        if (!finished) {
            if (!gotNext) {
                nextValue = getNext() ;
                if (finished) {
                    closeIfNeeded() ;
                }
                gotNext = true ;
            }
        }
         return !finished;
    }
    @Override
    public T next(){
        if (!hasNext()) {
            throw new NoSuchElementException("End of stream");
        }
        gotNext = false ;
        return nextValue ;
    }
}
