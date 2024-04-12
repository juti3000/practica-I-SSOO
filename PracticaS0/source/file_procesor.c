#include <pthread.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <time.h>
#include <stdbool.h>

struct Config {
    char directoriocomun[100];
    char fichconsolidados[100];
    char log[100];
    int numeroprocesos;
    int sleepmin;
    int sleepmax;
};

struct Config config;

void copiarContenido(FILE *origen, FILE *destino, char numeroSucursal, int *contadorLineas, const char *nombrearchivo);
void copiarContenidoLOG(FILE *origen, FILE *destino, int contadorLineas, const char *nombrearchivo);
void LeerFicheroconfiguracion(FILE *fichero);
void *Hilos(void *nombreArchivo);
void *DetectarPatrones(void *arg);

FILE *fichero_consolidados;
FILE *fichero_log;
FILE *fichero_patrones_irregulares;

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_patrones = PTHREAD_MUTEX_INITIALIZER;

int contadorLineas = 0;
bool seEncontraronPatrones = false;

pthread_t *p;

int main() {
    char linea[300];
    
    char *nombrearchivo;
    FILE *fichero = fopen("./fp.conf", "r");
    if (fichero == NULL) {
        printf("Error al abrir el archivo de configuración.\n");
        return -1;
    }
    LeerFicheroconfiguracion(fichero);
    fclose(fichero);

    p = malloc(config.numeroprocesos * sizeof(pthread_t));
    if (p == NULL) {
        printf("Error al asignar memoria para el arreglo de hilos.\n");
        return -1;
    }

    fichero_consolidados = fopen(config.fichconsolidados, "a");
    if (fichero_consolidados == NULL) {
        printf("Error al abrir el archivo de consolidados.\n");
        return -1;
    }

    fichero_log = fopen(config.log, "a");
    if (fichero_log == NULL){
        printf("Error al abrir el archivo de log. \n");
        return -1;
    }

    fichero_patrones_irregulares = fopen("patrones_irregulares.txt", "a");
    if (fichero_patrones_irregulares == NULL){
        printf("Error al abrir el archivo de patrones irregulares. \n");
        return -1;
    }

    DIR* directorio;
    struct dirent *archivo;

    directorio = opendir(config.directoriocomun);
    if (directorio == NULL) {
        printf("Error al abrir el directorio.\n");
        return -1;
    }
    
    int thread_index = 0;
    while ((archivo = readdir(directorio)) != NULL) {
        if (archivo->d_type == DT_REG) {
            char ruta[200] = "";
            strcat(ruta, config.directoriocomun);
            strcat(ruta, archivo->d_name);

            FILE *fichero_directorio = fopen(ruta, "r");
            if (fichero_directorio == NULL) {
                printf("Error al abrir el archivo %s.\n", archivo->d_name);
                continue;
            }
            printf("Procesando archivo: %s\n", archivo->d_name);
            char caracterNumero5 = archivo->d_name[4];
            switch (caracterNumero5) {
                case '1':
                case '2':
                case '3':
                case '4':
                    pthread_create(&p[thread_index], NULL, Hilos, strdup(ruta));
                    thread_index++;
                    break;
            }
            fclose(fichero_directorio);
            strcpy(ruta, "");
        }
    }
    closedir(directorio);

    for (int i = 0; i < thread_index; ++i) {
        pthread_join(p[i], NULL);
    }

    pthread_t patrones_thread;
    pthread_create(&patrones_thread, NULL, DetectarPatrones, NULL);
    pthread_join(patrones_thread, NULL);

    if (!seEncontraronPatrones) {
        printf("No se encontraron patrones irregulares.\n");
    }

    free(p);

    pthread_exit(NULL);
}

void *Hilos(void *nombreArchivo) {
    FILE *fichero_directorio = fopen((char *) nombreArchivo, "r");
    if (fichero_directorio == NULL) {
        printf("Error al abrir el archivo %s.\n", (char *) nombreArchivo);
        free(nombreArchivo);
        pthread_exit(NULL);
    }
    char *archivo = strrchr((char *) nombreArchivo, '/') + 1;
    char *nombre_archivo = (char *) nombreArchivo;
    copiarContenido(fichero_directorio, fichero_consolidados, nombre_archivo[12] , &contadorLineas, archivo);
    copiarContenidoLOG(fichero_directorio, fichero_log, contadorLineas, archivo);

    fclose(fichero_directorio);
    free(nombreArchivo);
    pthread_exit(NULL);
}

void *DetectarPatrones(void *arg) {
    FILE *consolidado = fopen(config.fichconsolidados, "r");
    if (consolidado == NULL) {
        printf("Error al abrir el archivo consolidado.\n");
        pthread_exit(NULL);
    }

    char linea[300];
    char *token;
    char *idUsuario;
    char *fechaHora;
    char *hora;
    char *fecha;
    int numTransaccionesHora = 0;
    char *lastUser = NULL;
    char *lastHour = NULL;
    int retirosDia = 0;
    int erroresDia = 0;
    bool operacionTipo[3] = {false, false, false}; 
    float ingresoDia = 0.0;
    float retiroDia = 0.0;
    bool patrones[5] = {false, false, false, false, false};

    while (fgets(linea, sizeof(linea), consolidado)) {
        token = strtok(linea, ";");
        token = strtok(NULL, ";");
        token = strtok(NULL, ";");
        token = strtok(NULL, ";");
        fechaHora = token;
        fecha = strtok(fechaHora, " ");
        hora = strtok(NULL, ":");

        token = strtok(NULL, ";");
        idUsuario = token;

        token = strtok(NULL, ";");
        if (strcmp(token, "COMPRA") == 0) {
            operacionTipo[0] = true;
        } else if (strcmp(token, "RETIRO") == 0) {
            operacionTipo[1] = true;
            retirosDia++;
        } else {
            operacionTipo[2] = true;
        }

        token = strtok(NULL, ";");
        token = strtok(NULL, ";");
        float importe = atof(token);
        if (importe > 0) {
            ingresoDia += importe;
        } else {
            retiroDia -= importe;
        }

        token = strtok(NULL, ";");

        if (lastUser == NULL || strcmp(idUsuario, lastUser) != 0 || strcmp(hora, lastHour) != 0) {
            if (numTransaccionesHora > 5) {
                printf("Patrón 1: Más de 5 transacciones para el usuario %s en la hora %s\n", lastUser, lastHour);
                patrones[0] = true;
            }
            lastUser = idUsuario;
            lastHour = hora;
            numTransaccionesHora = 0;
        }
        numTransaccionesHora++;

        if (retirosDia > 3) {
            printf("Patrón 2: Más de 3 retiros para el usuario %s en el día %s\n", lastUser, fecha);
            patrones[1] = true;
        }

        bool todasOperaciones = true;
        for (int i = 0; i < 3; ++i) {
            if (!operacionTipo[i]) {
                todasOperaciones = false;
                break;
            }
        }
        if (todasOperaciones) {
            printf("Patrón 4: El usuario %s realizó una operación de cada tipo en el día %s\n", lastUser, fecha);
            patrones[3] = true;
        }

        if (retiroDia > ingresoDia) {
            printf("Patrón 5: La cantidad de dinero retirado es mayor que el dinero ingresado para el usuario %s en el día %s\n", lastUser, fecha);
            patrones[4] = true;
        }
    }

    fclose(consolidado);

    pthread_mutex_lock(&mutex_patrones);
    for (int i = 0; i < 5; ++i) {
        if (patrones[i]) {
            switch (i) {
                case 0:
                    fprintf(fichero_patrones_irregulares, "Patrón 1: Más de 5 transacciones para un mismo usuario en una misma hora.\n");
                    break;
                case 1:
                    fprintf(fichero_patrones_irregulares, "Patrón 2: Más de 3 retiros para un mismo usuario en un mismo día.\n");
                    break;
                case 2:
                    fprintf(fichero_patrones_irregulares, "Patrón 3: Un usuario comete más de 3 errores en un mismo día.\n");
                    break;
                case 3:
                    fprintf(fichero_patrones_irregulares, "Patrón 4: Un usuario realiza una operación de cada tipo en un mismo día.\n");
                    break;
                case 4:
                    fprintf(fichero_patrones_irregulares, "Patrón 5: La cantidad de dinero retirado es mayor que el dinero ingresado en un mismo día.\n");
                    break;
            }
        }
    }
    pthread_mutex_unlock(&mutex_patrones);

    pthread_exit(NULL);
}

void copiarContenidoLOG(FILE *origen, FILE *destino, int contadorLineas, const char *nombrearchivo) {
    time_t rawtime;
    struct tm *fechaHoraActual;
    char fechaHora[80];

    time(&rawtime);
    fechaHoraActual = localtime(&rawtime);

    pthread_mutex_lock(&mutex);
    fprintf( destino, "%d:%d:%d:::%d:::%s\n",fechaHoraActual->tm_hour, fechaHoraActual->tm_min, fechaHoraActual->tm_sec, contadorLineas, nombrearchivo);
    pthread_mutex_unlock(&mutex);
}

void copiarContenido(FILE *origen, FILE *destino, char numeroSucursal, int *contadorLineas, const char *nombrearchivo) {
    char linea[100];
   
    (*contadorLineas) = 0;

    while (fgets(linea, sizeof(linea), origen)) {
        pthread_mutex_lock(&mutex);
        fprintf(destino,"%c;%s", numeroSucursal, linea);
        pthread_mutex_unlock(&mutex);
        (*contadorLineas)++;
    }

    pthread_mutex_lock(&mutex);
    fprintf(destino, "\n");
    pthread_mutex_unlock(&mutex);
}

void LeerFicheroconfiguracion(FILE *fichero) {
    int contador = 0;
    char linea[300];

    while (fgets(linea, sizeof(linea), fichero)) {
        char *token = strtok(linea, "=");
        while (token != NULL) {
            token[strcspn(token, "\n")] = '\0';

            switch (contador) {
                case 1:
                    strcpy(config.directoriocomun, token);
                    break;

                case 3:
                    strcpy(config.fichconsolidados, token);
                    break;

                case 5:
                    strcpy(config.log, token);
                    break;

                case 7:
                    config.numeroprocesos = atoi(token);
                    break;

                case 9:
                    config.sleepmin = atoi(token);
                    break;

                case 11:
                    config.sleepmax = atoi(token);
                    break;
            }
            contador++;
            token = strtok(NULL, "=");
        }
    }
}