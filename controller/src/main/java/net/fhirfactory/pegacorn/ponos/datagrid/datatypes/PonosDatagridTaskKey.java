/*
 * Copyright (c) 2021 Mark A. Hunter
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package net.fhirfactory.pegacorn.ponos.datagrid.datatypes;

import com.fasterxml.jackson.annotation.JsonIgnore;
import net.fhirfactory.pegacorn.core.interfaces.datagrid.DatagridElementKeyInterface;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.identity.datatypes.TaskIdType;

import java.io.Serializable;
import java.util.Objects;

public class PonosDatagridTaskKey implements DatagridElementKeyInterface, Serializable {
    private TaskIdType taskId;

    //
    // Constructor(s)
    //

    public PonosDatagridTaskKey(){
        this.taskId = null;
    }

    public PonosDatagridTaskKey(TaskIdType taskId){
        this.taskId = taskId;
    }

    //
    // Getters and Setters
    //

    public TaskIdType getTaskId() {
        return taskId;
    }

    public void setTaskId(TaskIdType taskId) {
        this.taskId = taskId;
    }

    //
    // Implemented Methods
    //

    @Override
    @JsonIgnore
    public String getKey() {
        if(taskId != null){
            if(taskId.getLocalId() != null){
                String id = taskId.getLocalId();
                return(id);
            }
        }
        return(null);
    }

    //
    // Hash and Equals
    //

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PonosDatagridTaskKey that = (PonosDatagridTaskKey) o;
        return Objects.equals(getTaskId(), that.getTaskId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getTaskId());
    }

    //
    // To String
    //

    @Override
    public String toString() {
        return "PonosDatagridTaskKey{" +
                "taskId=" + taskId +
                '}';
    }
}
