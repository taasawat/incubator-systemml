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
/*
package org.apache.sysml.runtime.instructions.gpu;

import org.apache.sysml.parser.Expression.DataType;
import org.apache.sysml.parser.Expression.ValueType;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.functionobjects.DiagIndex;
import org.apache.sysml.runtime.functionobjects.RevIndex;
import org.apache.sysml.runtime.functionobjects.SortIndex;
import org.apache.sysml.runtime.functionobjects.SwapIndex;
import org.apache.sysml.runtime.instructions.InstructionUtils;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.instructions.cp.UnaryCPInstruction;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.operators.Operator;
import org.apache.sysml.runtime.matrix.operators.ReorgOperator;

*/
//public class ReorgGPUInstruction extends GPUInstruction
//{
	/*
	//sort-specific attributes (to enable variable attributes)
 	private CPOperand _col = null;
 	private CPOperand _desc = null;
 	private CPOperand _ixret = null;
 	
 	private CPOperand input1, output;
 	*/
 	/**
 	 * for opcodes r' and rdiag
 	 * 
 	 * @param op
 	 * @param in
 	 * @param out
 	 * @param opcode
 	 * @param istr
 	 */
 	/*
	public ReorgGPUInstruction(Operator op, CPOperand in, CPOperand out, String opcode, String istr){
		super(op, in, out, opcode, istr);
		_gputype = GPUINSTRUCTION_TYPE.Reorg;
	}
	*/
	/**
	 * for opcode rsort
	 * 
	 * @param op
	 * @param in
	 * @param col
	 * @param desc
	 * @param ixret
	 * @param out
	 * @param opcode
	 * @param istr
	 */
	/*
	public ReorgGPUInstruction(Operator op, CPOperand in, CPOperand col, CPOperand desc, CPOperand ixret, CPOperand out, String opcode, String istr){
		this(op, in, out, opcode, istr);
		_col = col;
		_desc = desc;
		_ixret = ixret;
	}
	*/
	/*
	public static ReorgGPUInstruction parseInstruction ( String str ) 
		throws DMLRuntimeException 
	{
		CPOperand in = new CPOperand("", ValueType.UNKNOWN, DataType.UNKNOWN);
		CPOperand out = new CPOperand("", ValueType.UNKNOWN, DataType.UNKNOWN);
		
		String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
		String opcode = parts[0];
		
		if ( opcode.equalsIgnoreCase("r'") ) {
			InstructionUtils.checkNumFields(str, 2, 3);
			in.split(parts[1]);
			out.split(parts[2]);
			int k = Integer.parseInt(parts[3]);
			return new ReorgGPUInstruction(new ReorgOperator(SwapIndex.getSwapIndexFnObject(), k), in, out, opcode, str);
		}
		else {
			throw new DMLRuntimeException("Unknown opcode while parsing a GPU ReorgInstruction: " + str);
		}
	}
	*/
	/*
	@Override
	public void processInstruction(ExecutionContext ec)
			throws DMLRuntimeException 
	{
		//acquire inputs
		MatrixBlock matBlock = ec.getMatrixInput(input1.getName());		
		ReorgOperator r_op = (ReorgOperator) _optr;
		if( r_op.fn instanceof SortIndex ) {
			//additional attributes for sort
			int col = (int)ec.getScalarInput(_col.getName(), _col.getValueType(), _col.isLiteral()).getLongValue();
			boolean desc = ec.getScalarInput(_desc.getName(), _desc.getValueType(), _desc.isLiteral()).getBooleanValue();
			boolean ixret = ec.getScalarInput(_ixret.getName(), _ixret.getValueType(), _ixret.isLiteral()).getBooleanValue();
			r_op.fn = SortIndex.getSortIndexFnObject(col, desc, ixret);
		}
		
		//execute operation
		MatrixBlock soresBlock = (MatrixBlock) (matBlock.reorgOperations(r_op, new MatrixBlock(), 0, 0, 0));
        
		//release inputs/outputs
		ec.releaseMatrixInput(input1.getName());
		ec.setMatrixOutput(output.getName(), soresBlock);
	}
	*/
//}