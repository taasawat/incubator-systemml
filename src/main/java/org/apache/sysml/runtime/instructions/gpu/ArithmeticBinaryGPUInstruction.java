package org.apache.sysml.runtime.instructions.gpu;


import org.apache.sysml.parser.Expression.DataType;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.instructions.InstructionUtils;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.matrix.operators.Operator;

public abstract class ArithmeticBinaryGPUInstruction extends GPUInstruction {

	protected CPOperand _input1;
	protected CPOperand _input2;
	protected CPOperand _output;

	public ArithmeticBinaryGPUInstruction(Operator op, CPOperand in1, CPOperand in2, CPOperand out, String opcode, String istr) {
		super(op, opcode, istr);
		_gputype = GPUINSTRUCTION_TYPE.ArithmeticBinary;
		_input1 = in1;
		_input2 = in2;
	    _output = out;
	}
	
	public static ArithmeticBinaryGPUInstruction parseInstruction ( String str ) throws DMLRuntimeException {
		String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
		InstructionUtils.checkNumFields ( parts, 3 );
		
		String opcode = parts[0];
		CPOperand in1 = new CPOperand(parts[1]);
		CPOperand in2 = new CPOperand(parts[2]);
		CPOperand out = new CPOperand(parts[3]);
		
		DataType dt1 = in1.getDataType();
		DataType dt2 = in2.getDataType();
		DataType dt3 = out.getDataType();
	 
		Operator operator = InstructionUtils.parseBinaryOperator(opcode);
		
		if(dt1 == DataType.MATRIX && dt2 == DataType.MATRIX && dt3 == DataType.MATRIX) {
			if((opcode.equalsIgnoreCase("+") || opcode.equalsIgnoreCase("-")))
				return new MatrixMatrixArithmeticGPUInstruction(operator, in1, in2, out, opcode, str);
			else
				throw new DMLRuntimeException("Unsupported GPU ArithmeticInstruction. :: " + out.getDataType() + " = " + in1.getDataType() + " " + operator + " " + in1.getDataType() );	
		}
		else if( dt3 == DataType.MATRIX && ((dt1 == DataType.SCALAR && dt2 == DataType.MATRIX) || (dt1 == DataType.MATRIX && dt2 == DataType.SCALAR)) ) {
			if((opcode.equalsIgnoreCase("*") || opcode.equalsIgnoreCase("*2") || opcode.equalsIgnoreCase("/")))
				return new MatrixScalarArithmeticGPUInstruction(operator, in1, in2, out, opcode, str);
			else
				throw new DMLRuntimeException("Unsupported GPU ArithmeticInstruction. :: " + out.getDataType() + " = " + in1.getDataType() + " " + operator + " " + in1.getDataType() );
		}
		else
			throw new DMLRuntimeException("Unsupported GPU ArithmeticInstruction.");
	}
}