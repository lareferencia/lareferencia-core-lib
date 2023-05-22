package org.lareferencia.core.tests;


import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.lareferencia.core.monitor.LoadingProcessMonitorService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;



@SpringBootTest
public class LoadingProcessMonitorServiceTest {
    
    @Autowired
    private LoadingProcessMonitorService monitorService;

    
    @Test
    public void testIsLoadingProcessInProgress() {
        boolean loadingInProgress = monitorService.isLoadingProcessInProgress();
        Assertions.assertFalse(loadingInProgress, "Expected loading process to be in progress");
    }
    
    @Test
    public void testGetLoadedEntities() {
        int loadedEntities = monitorService.getLoadedEntities();
        Assertions.assertEquals(0, loadedEntities, "Expected 0 loaded entities");
    }
    
    // Adicione outros métodos de teste para testar outros atributos e métodos do serviço
    
}
