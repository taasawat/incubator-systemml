package org.apache.sysml.runtime.instructions.gpu;

import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.functionobjects.Plus;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.matrix.data.LibMatrixCUDA;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.operators.BinaryOperator;
import org.apache.sysml.runtime.matrix.operators.Operator;
import org.apache.sysml.utils.Statistics;

public class MatrixMatrixArithmeticGPUInstruction extends ArithmeticBinaryGPUInstruction
{
	
	public MatrixMatrixArithmeticGPUInstruction(Operator op, 
											   CPOperand in1, 
											   CPOperand in2, 
											   CPOperand out, 
											   String opcode,
											   String istr){
		super(op, in1, in2, out, opcode, istr);
	}
	
	@Override
	public void processInstruction(ExecutionContext ec) throws DMLRuntimeException {
		Statistics.incrementNoOfExecutedGPUInst();
		
		MatrixObject in1 = ec.getMatrixInputForGPUInstruction(_input1.getName());
		MatrixObject in2 = ec.getMatrixInputForGPUInstruction(_input2.getName());
		
		//TODO: make hop level changes for this
		boolean isLeftTransposed = true;
		boolean isRightTransposed = true;
		int rlen = isLeftTransposed ? (int) in1.getNumColumns() : (int) in1.getNumRows();
		int clen = isLeftTransposed ? (int) in1.getNumRows() : (int) in1.getNumColumns();
		
		ec.setMetaData(_output.getName(), rlen, clen);
		MatrixObject out = ec.getMatrixOutputForGPUInstruction(_output.getName(), false);
		
		BinaryOperator bop = (BinaryOperator) _optr;
		boolean isAdd = bop.fn instanceof Plus;
		LibMatrixCUDA.cellwiseMatMatAddSub(in1, in2, out, isLeftTransposed, isRightTransposed, isAdd);
		
		ec.releaseMatrixInputForGPUInstruction(_input1.getName());
		ec.releaseMatrixInputForGPUInstruction(_input2.getName());
        ec.releaseMatrixOutputForGPUInstruction(_output.getName());
	}
}