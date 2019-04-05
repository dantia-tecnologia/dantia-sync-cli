import { TestBed } from '@angular/core/testing';

import { DantiaSyncCliService } from './dantia-sync-cli.service';

describe('DantiaSyncCliService', () => {
  beforeEach(() => TestBed.configureTestingModule({}));

  it('should be created', () => {
    const service: DantiaSyncCliService = TestBed.get(DantiaSyncCliService);
    expect(service).toBeTruthy();
  });
});
