#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>

#define DEBUG 1       // comentar esta linha quando for medir tempo
#define ARRAY_SIZE 40 // trabalho final com o valores 10.000, 100.000, 1.000.000

void bs(int n, int *vetor)
{
  int c = 0, d, troca, trocou = 1;

  while (c < (n - 1) & trocou)
  {
    trocou = 0;
    for (d = 0; d < n - c - 1; d++)
      if (vetor[d] > vetor[d + 1])
      {
        troca = vetor[d];
        vetor[d] = vetor[d + 1];
        vetor[d + 1] = troca;
        trocou = 1;
      }
    c++;
  }
}

void Mostra(int *vetor, int tam)
{
#ifdef DEBUG
  printf("\nVetor ordenado: ");
  for (int i = 0; i < tam; i++)
    printf("[%03d] ", vetor[i]);
  printf("\n");
#endif
}

void Inicializa(int *vetor, int tam)
{
  for (int i = 0; i < tam; i++)
  {
    vetor[i] = tam - i;
  }
}

int *interleaving(int vetor[], int tam)
{
  int *vetor_auxiliar;
  int i1, i2, i_aux;

  vetor_auxiliar = (int *)malloc(sizeof(int) * tam);

  i1 = 0;
  i2 = tam / 2;

  for (i_aux = 0; i_aux < tam; i_aux++)
  {
    if (((vetor[i1] <= vetor[i2]) && (i1 < (tam / 2))) || (i2 == tam))
      vetor_auxiliar[i_aux] = vetor[i1++];
    else
      vetor_auxiliar[i_aux] = vetor[i2++];
  }

  return vetor_auxiliar;
}

void main(int argc, char **argv)
{

  int tam_vetor = ARRAY_SIZE;
  int *vetor;
  int delta = 10;
  int my_rank, num_procs;
  MPI_Status status;

  if (argc > 1)
  {
    int input_size = atoi(argv[1]);
    if (input_size > 0)
    {
      tam_vetor = input_size;
    }
  }

  vetor = (int *)malloc(sizeof(int) * tam_vetor);

  MPI_Init(&argc, &argv);

  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
  MPI_Comm_size(MPI_COMM_WORLD, &num_procs);

  int pai = (my_rank - 1) / 2;
  int filho_esq = 2 * my_rank + 1;
  int filho_dir = 2 * my_rank + 2;

  if (my_rank == 0)
  {
    Inicializa(vetor, tam_vetor);
  }

  // Recebe vetor se não for raiz
  if (my_rank != 0)
  {
    MPI_Probe(pai, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
    tam_vetor = status.MPI_TAG;
    free(vetor); // libera vetor alocado anteriormente
    vetor = (int *)malloc(sizeof(int) * tam_vetor);
    MPI_Recv(vetor, tam_vetor, MPI_INT, pai, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
    MPI_Get_count(&status, MPI_INT, &tam_vetor); // descubro tamanho da mensagem recebida
  }

  if (tam_vetor <= delta || filho_esq >= num_procs)
  {
    bs(tam_vetor, vetor);
  }
  else
  {
    int metade = tam_vetor / 2;

    // Envia para filhos
    MPI_Send(&vetor[0], metade, MPI_INT, filho_esq, metade, MPI_COMM_WORLD);
    MPI_Send(&vetor[metade], metade, MPI_INT, filho_dir, metade, MPI_COMM_WORLD);

    // Recebe dos filhos
    MPI_Recv(&vetor[0], metade, MPI_INT, filho_esq, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
    MPI_Recv(&vetor[metade], metade, MPI_INT, filho_dir, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

    // Intercala as duas metades
    int *temp = interleaving(vetor, tam_vetor);
    vetor = temp;
  }

  // Se não for raiz, envia vetor ordenado para o pai
  if (my_rank != 0)
  {
    MPI_Send(vetor, tam_vetor, MPI_INT, pai, 0, MPI_COMM_WORLD);
  }
  else
  {
    Mostra(vetor, tam_vetor);
  }

  MPI_Finalize();
}
