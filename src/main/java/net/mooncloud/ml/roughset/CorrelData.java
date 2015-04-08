package net.mooncloud.ml.roughset;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class CorrelData
{
	public static int[][] correl47(int n)
	{
		int BASE = 0b1111, superscript;
		int data[][] = new int[n * (BASE + 1)][48];
		int A0, A1, B0, B1, I1, I2, R, Y;
		Random r = new Random();
		for (int i = 0; i < n; i++)
		{
			for (int j = 0; j <= BASE; j++)
			{
				A0 = (j & 1) == 0 ? 0 : 1;
				A1 = (j & 2) == 0 ? 0 : 1;
				B0 = (j & 4) == 0 ? 0 : 1;
				B1 = (j & 8) == 0 ? 0 : 1;
				Y = (A0 & A1) | (B0 & B1);

				R = r.nextDouble() < 0.75 ? Y : 1 - Y;
				I1 = r.nextDouble() < 0.5 ? 0 : 1;
				I2 = r.nextDouble() < 0.5 ? 0 : 1;

				superscript = i * (BASE+1) + j;

				data[superscript][0] = A0;
				data[superscript][1] = A1;
				data[superscript][2] = B0;
				data[superscript][3] = B1;
				data[superscript][4] = R;

				for (int k = 5; k < 12; k++)
				{
					data[superscript][k] = (r.nextDouble() < 1 - (3 / 100.0)) ? I1 : 1 - I1;
				}
				for (int k = 12; k < 19; k++)
				{
					data[superscript][k] = (r.nextDouble() < 1 - (3 / 100.0)) ? I2 : 1 - I2;
				}

				for (int k = 19, l = 0; k < 26; k++, l++)
				{
					data[superscript][k] = (r.nextDouble() < 1 - (l / 16.0)) ? A0 : 1 - A0;
				}
				for (int k = 26, l = 0; k < 33; k++, l++)
				{
					data[superscript][k] = (r.nextDouble() < 1 - (l / 16.0)) ? A1 : 1 - A1;
				}
				for (int k = 33, l = 0; k < 40; k++, l++)
				{
					data[superscript][k] = (r.nextDouble() < 1 - (l / 16.0)) ? B0 : 1 - B0;
				}
				for (int k = 40, l = 0; k < 47; k++, l++)
				{
					data[superscript][k] = (r.nextDouble() < 1 - (l / 16.0)) ? B1 : 1 - B1;
				}

				data[superscript][47] = Y;
			}
		}

		return data;
	}

	public static void main(String[] args) throws IOException
	{
		FileWriter fileWriter = new FileWriter("correl47data", false);
		BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
		int data[][] = correl47(10240 / 16);
		for (int i = 0; i < data.length; i++)
		{
			StringBuilder sb = new StringBuilder();
			sb.append(data[i][0]);
			for (int j = 1; j < data[i].length; j++)
			{
				sb.append(",").append(data[i][j]);
			}
			System.out.println(sb);
			bufferedWriter.write(sb.toString() + "\n");
		}
		bufferedWriter.close();
		fileWriter.close();
	}
}
