/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sysml.runtime.instructions.gpu;


import org.apache.sysml.parser.Expression.DataType;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.operators.BinaryOperator;
import org.apache.sysml.runtime.matrix.operators.Operator;
import org.apache.sysml.runtime.matrix.operators.ScalarOperator;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysml.runtime.functionobjects.Divide;
import org.apache.sysml.runtime.functionobjects.Multiply;
import org.apache.sysml.runtime.functionobjects.Multiply2;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.instructions.cp.ScalarObject;
import org.apache.sysml.runtime.matrix.data.LibMatrixCUDA;
import org.apache.sysml.runtime.matrix.data.LibMatrixCUDA.GPUEnabledElementwiseOp;
//import org.apache.sysml.runtime.matrix.operators.BinaryOperator;
import org.apache.sysml.utils.Statistics;

public class MatrixScalarArithmeticGPUInstruction extends ArithmeticBinaryGPUInstruction{
	public MatrixScalarArithmeticGPUInstruction(Operator op, 
			   									CPOperand in1, 
			   									CPOperand in2, 
			   									CPOperand out, 
			   									String opcode,
			   									String istr){
		super(op, in1, in2, out, opcode, istr);
	}
	
	@Override
	public void processInstruction(ExecutionContext ec) 
		throws DMLRuntimeException
	{
		Statistics.incrementNoOfExecutedGPUInst();
		
		CPOperand mat = ( _input1.getDataType() == DataType.MATRIX ) ? _input1 : _input2;
		CPOperand scalar = ( _input1.getDataType() == DataType.MATRIX ) ? _input2 : _input1;
		MatrixObject in1 = ec.getMatrixInputForGPUInstruction(mat.getName());
		ScalarObject constant = (ScalarObject) ec.getScalarInput(scalar.getName(), scalar.getValueType(), scalar.isLiteral());
		
		ec.setMetaData(_output.getName(), in1.getNumRows(), in1.getNumColumns());
		MatrixObject out = ec.getMatrixOutputForGPUInstruction(_output.getName(), false);
		
		GPUEnabledElementwiseOp op;
		if(_optr instanceof BinaryOperator) {
			if(((BinaryOperator)_optr).fn instanceof Multiply || ((BinaryOperator)_optr).fn instanceof Multiply2) {
				op = GPUEnabledElementwiseOp.MULTIPLY;
			}
			else if(((BinaryOperator)_optr).fn instanceof Divide) {
				op = GPUEnabledElementwiseOp.DIVIDE;
			}
			else {
				throw new DMLRuntimeException("The operator is not supported");
			}
		}
		else {
			throw new DMLRuntimeException("The operator is not supported");
		}
		
		LibMatrixCUDA.matScalarElementwiseMultDiv(in1, constant.getDoubleValue(), out, op);
		
		ec.releaseMatrixInputForGPUInstruction(mat.getName());
        ec.releaseMatrixOutputForGPUInstruction(_output.getName());
	
	}
}


/*
	
		MatrixBlock retBlock = (MatrixBlock) inBlock.scalarOperations(sc_op, new MatrixBlock());
		
		ec.releaseMatrixInput(mat.getName());
		
		// Ensure right dense/sparse output representation (guarded by released input memory)
		if( checkGuardedRepresentationChange(inBlock, retBlock) ) {
 			retBlock.examSparsity();
 		}
		
		ec.setMatrixOutput(output.getName(), retBlock);
	}
}



*/